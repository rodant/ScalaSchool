package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.Some

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedSnapshotSeq = 0
  var persistence = context.actorOf(persistenceProps)
  // pending acknowledgments: map from sequence number and receiver to pair of requester and message
  var acks = Map.empty[(Long, ActorRef), (ActorRef, Any)]

  @throws(classOf[Exception])
  override def preStart(): Unit = {
    arbiter ! Join
    context.watch(persistence)
  }

  @throws(classOf[Exception])
  override def postStop(): Unit = {
    context.unwatch(persistence)
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  private val retryStep = 100
  private val globalTimeout: Int = 1000

  private def sendPersist(id: Long, key: String, optionValue: Option[String]) {
    acks += (id, persistence) ->(sender, Persist(key, optionValue, id))
    persistence ! Persist(key, optionValue, id)
    context.system.scheduler.scheduleOnce(retryStep millis, self, (id, persistence, retryStep))
  }

  private def persistenceRetryBehavior(ackMessage: (String, Long) => Any): PartialFunction[Any, Unit] = {
    case (id: Long, receiver: ActorRef, count: Int) if receiver == persistence && acks.contains((id, receiver)) => {
      if (count == globalTimeout) {
        acks((id, receiver))._1 ! OperationFailed(id)
        acks -= ((id, receiver))
      } else {
        receiver ! acks((id, receiver))._2
        context.system.scheduler.scheduleOnce(retryStep millis, self, (id, receiver, count + retryStep))
      }
    }

    case Persisted(key, id) if acks contains((id, persistence)) => {
      val requestor = acks((id, persistence))._1
      acks -= ((id, persistence))
      requestor ! ackMessage(key, id)//OperationAck(id)
    }
  }

  /* TODO Behavior for  the leader role. */
  private val primaryBehavior: PartialFunction[Any, Unit] = {
    case Insert(key, value, id) => {
      kv += key -> value
      sendPersist(id, key, Some(value))
    }
    case Remove(key, id) => {
      kv -= key
      sendPersist(id, key, None)
    }

    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
  }

  val leader: Receive = primaryBehavior orElse persistenceRetryBehavior((key: String, id: Long) => OperationAck(id))

  /* TODO Behavior for the replica role. */
  private val secondaryBehavior: PartialFunction[Any, Unit] = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOption, seq) => {
      if (seq == expectedSnapshotSeq) {
        log.info(s"*** Snapshot, $seq == $expectedSnapshotSeq")
        expectedSnapshotSeq += 1
        valueOption match {
          case Some(value) => kv += key -> value
          case None => kv -= key
        }
        sendPersist(seq, key, valueOption)
      }
      else if (seq < expectedSnapshotSeq) {
        log.info(s"*** Snapshot, $seq < $expectedSnapshotSeq")
        sender ! SnapshotAck(key, seq)
      }
    }

    case Terminated(persistor) => {
      persistence = context.actorOf(persistenceProps)
    }
  }

  val replica: Receive = secondaryBehavior orElse persistenceRetryBehavior(SnapshotAck)
}
