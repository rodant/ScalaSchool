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
  // map from sequence number and receiver to pair of requester and request
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

  def sendPersist(id: Long, key: String, optionValue: Option[String]) {
    acks += (id, persistence) ->(sender, Persist(key, optionValue, id))
    persistence ! Persist(key, optionValue, id)
    context.system.scheduler.scheduleOnce(100 millis, self, (id, persistence, 1))
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv += key -> value
      sendPersist(id, key, Some(value))
    }
    case Remove(key, id) => {
      kv -= key
      sendPersist(id, key, None)
    }

    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case (id: Long, actor: ActorRef, count: Int) if actor == persistence && acks.contains((id, actor)) => {
      if (count == 10) {
        acks((id, actor))._1 ! OperationFailed(id)
        acks -= ((id, actor))
      } else {
        persistence ! acks((id, actor))._2
        context.system.scheduler.scheduleOnce(100 millis, self, (id, actor, count + 1))
      }
    }

    case Persisted(key, id) if acks contains((id, persistence)) => {
      val requestor = acks((id, persistence))._1
      acks -= ((id, persistence))
      requestor ! OperationAck(id)
    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
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
        /*acks += (seq, persistence) -> (sender, Persist(key, valueOption, seq))
        persistence ! Persist(key, valueOption, seq)
        context.system.scheduler.scheduleOnce(100 millis, self, (seq, persistence))*/
      }
      else if (seq < expectedSnapshotSeq) {
        log.info(s"*** Snapshot, $seq < $expectedSnapshotSeq")
        sender ! SnapshotAck(key, seq)
      }
    }

    case (seq: Long, persistence: ActorRef, count: Int) if acks contains((seq, persistence)) => {
      log.info(s"*** Timeout, $seq")
      if (count == 10) {
        acks((seq, persistence))._1 ! OperationFailed(seq)
        acks -= ((seq, persistence))
      } else {
        persistence ! acks((seq, persistence))._2
        context.system.scheduler.scheduleOnce(100 millis, self, (seq, persistence, count + 1))
      }
      log.info(s"*** Persist again, $seq")
    }

    case Persisted(key, seq) if acks contains ((seq, persistence)) => {
      val requester = acks((seq, persistence))._1
      acks -= ((seq, persistence))
      requester ! SnapshotAck(key, seq)
      log.info(s"*** Persisted. Sent SnapshotAck($key, $seq).")
    }

    case Terminated(persistor) => {
      persistence = context.actorOf(persistenceProps)
    }
  }

}
