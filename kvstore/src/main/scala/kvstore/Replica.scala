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
  // the next sequence number for each replicator
  var replicatorToNextSeq = Map.empty[ActorRef, Long]
  // (seq, replicator) -> id for pending operations
  var seqReplicatorToPendingId = Map.empty[(Long, ActorRef), Long]

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

  private val retryInterval = 100
  private val retryTimeout: Int = 1000

  private def sendInitialMessage(id: Long, receiver: ActorRef, message: Any) {
    acks += (id, receiver) ->(sender, message)
    receiver ! message
    context.system.scheduler.scheduleOnce(retryInterval millis, self, (id, receiver, retryInterval))
  }

  private def retryBehavior(ackMessage: (String, Long) => Any): PartialFunction[Any, Unit] = {
    case (id: Long, receiver: ActorRef, count: Int) if acks.contains((id, receiver)) => {
      if (count == retryTimeout) {
        acks((id, receiver))._1 ! OperationFailed(id)
        acks -= ((id, receiver))
      } else {
        receiver ! acks((id, receiver))._2
        context.system.scheduler.scheduleOnce(retryInterval millis, self, (id, receiver, count + retryInterval))
      }
    }

    case Persisted(key, id) if acks contains((id, persistence)) => {
      val requestor = acks((id, persistence))._1
      acks -= ((id, persistence))
      if (noPendingReplications(id)) {
        requestor ! ackMessage(key, id)
      }
    }

    case Replicated(key, seq) if seqReplicatorToPendingId.contains(seq, sender) => {
      val id = seqReplicatorToPendingId(seq, sender)
      val requestor = acks((id, sender))._1
      acks -= ((id, sender))
      seqReplicatorToPendingId -= ((seq, sender))
      if (noPendingReplications(id) && !acks.contains((id, persistence))) {
        requestor ! ackMessage(key, id)
      }
    }
  }

  private def noPendingReplications(id: Long): Boolean = replicators.forall(!acks.contains(id, _))

  /* TODO Behavior for  the leader role. */
  private def sendInitialToReplicators(id: Long, key: String, optionValue: Option[String]) {
    replicators foreach { replicator =>
        val seq = replicatorToNextSeq(replicator)
        sendInitialMessage(id, replicator, Replicate(key, optionValue, seq))
        replicatorToNextSeq += replicator -> (seq + 1)
        seqReplicatorToPendingId += (seq, replicator) -> id
    }
  }

  private val primaryBehavior: PartialFunction[Any, Unit] = {
    case Insert(key, value, id) => {
      kv += key -> value
      sendInitialMessage(id, persistence, Persist(key, Some(value), id))
      sendInitialToReplicators(id, key, Some(value))
    }
    case Remove(key, id) => {
      kv -= key
      sendInitialMessage(id, persistence, Persist(key, None, id))
      sendInitialToReplicators(id, key, None)
    }

    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case Replicas(replicas) => {
      log.info(s"**** Replicas $replicas")
      replicators = replicas.filter(_ != self) map(replica => context.actorOf(Replicator.props(replica)))
      replicators foreach {
        replicatorToNextSeq += _ -> 0
      }
    }
  }

  val leader: Receive = primaryBehavior orElse retryBehavior((key: String, id: Long) => OperationAck(id))

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
        sendInitialMessage(seq, persistence, Persist(key, valueOption, seq))
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

  val replica: Receive = secondaryBehavior orElse retryBehavior(SnapshotAck)
}
