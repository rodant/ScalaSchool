package kvstore

import akka.actor.{Cancellable, Props, Actor, ActorRef}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import akka.util.Timeout

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def sendSnapshot(req: Replicator.Replicate, seq: Long) {
    replica ! Snapshot(req.key, req.valueOption, seq)
    context.system.scheduler.scheduleOnce(100 millis, self, seq)
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case req: Replicate => {
      val seq = nextSeq
      acks += seq -> (sender, req)
      sendSnapshot(req, seq)
    }
    case SnapshotAck(key, seq) => {
      val senderRequest: Option[(ActorRef, Replicate)] = acks.get(seq)
      senderRequest match {
        case Some((sender, req)) => {
          acks -= seq
          sender ! Replicated(key, req.id)
        }
        case None =>
      }
    }
    case seq: Long if acks contains(seq) => {
      val req = acks(seq)._2
      sendSnapshot(req, seq)
    }
  }

}
