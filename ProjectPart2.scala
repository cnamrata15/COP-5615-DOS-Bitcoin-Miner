
import akka.actor._
import akka.actor.Actor
import akka.actor.Props
import scala.util.Random
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit;
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory

object ProjectPart2 {

  final var GatorId = "rohitnair";
  var required_time_in_mins = 5;
  var no_of_actors = Runtime.getRuntime().availableProcessors();
  var default_zero_length = 3;
  var server_ip = ""

  def main(args: Array[String]) {
    var server = true;
    var zero_length = 0;
    var mins = required_time_in_mins
    if (args.length == 0) {
      zero_length = default_zero_length;
    } else {
      if (args(0).toString().contains(".")) {
        server = false
      } else {
        zero_length = args(0).toInt;
      }
    }
    if (server == true) {
      println("STARTING AS SERVER...")
      var zeroes_count = zero_length
      var zeroes = ""
      while (zeroes_count != 0) {
        zeroes = zeroes + "0"
        zeroes_count -= 1;
      }

      println(new java.io.File(".").getCanonicalPath)
      val source = scala.io.Source.fromFile("application.conf")
      val lines = source.mkString
      source.close()

      val myConfig = ConfigFactory.load(ConfigFactory.parseString(lines))
      val system = ActorSystem("project1System", myConfig)

      var act_count = 0
      var helloActors = new Array[akka.actor.ActorRef](no_of_actors)
      var controller = system.actorOf(Props[controllerActor], name = "CONTROLLER_ACTOR");
      while (act_count < no_of_actors) {
        helloActors(act_count) = system.actorOf(Props[worker]);
        val actorId = act_count
        val params = new startWork(no_of_actors, actorId, GatorId, zeroes, controller, required_time_in_mins)
        //	    helloActor ! "Test"; 
        import system.dispatcher
        helloActors(actorId) ! params;
        //	    system.scheduler.scheduleOnce(required_time_in_mins minutes, helloActors(actorId), new StopMessage(actorId))    
        act_count += 1
        controller ! params
      }
      controller ! new SysAndActors(system, helloActors)
    } else {
      server_ip = args(0).toString()
      println("STARTING AS CLIENT WITH " + server_ip + " AS SERVER...")

      val source = scala.io.Source.fromFile("client.conf")
      val lines = source.mkString
      source.close()

      val myConfig = ConfigFactory.load(ConfigFactory.parseString(lines))

      val system = ActorSystem("HelloRemoteSystem", myConfig)
      val count = Runtime.getRuntime().availableProcessors();
      var remoteActor = new Array[akka.actor.ActorRef](count)
      var index = 0
      var rem_act_count = new remoteActorCount(count)
      while (index < count) {
        remoteActor(index) = system.actorOf(Props[LocalActor])
        remoteActor(index) ! rem_act_count
        index += 1
      }
    }
  }

  def getIP(): String = {
    server_ip
  }
}

sealed trait Project1Message
case class startWork(no_of_actors: Int, actor_id: Int, GatorId: String, zeroes: String, controller: akka.actor.ActorRef, req_time: Int) extends Project1Message
case class processMsg(actor_id: Int, Str: String, Hash: String) extends Project1Message
case class StopMessage(actor_id: Int) extends Project1Message
case class SysAndActors(SysId: akka.actor.ActorSystem, actors: Array[akka.actor.ActorRef]) extends Project1Message
case class AnActorStopped(ActorId: Int, actor: akka.actor.ActorRef) extends Project1Message
case class remoteConnect() extends Project1Message
case class remoteAssignID(id: Int, GatorId: String, zeroes: String, req_time: Int) extends Project1Message
case class remoteActorCount(count: Int) extends Project1Message

class controllerActor extends Actor {
  var mine_count = 0
  var current_actor_count = 0;
  var zeroes = ""
  var gatorId = ""
  var reqTime = 0

  def receive = {
    case param: startWork =>
      zeroes = param.zeroes
      gatorId = param.GatorId
      reqTime = param.req_time
    case processmsg: processMsg =>
      println(processmsg.Str + "\t" + processmsg.Hash);
      mine_count += 1;
    case actors: SysAndActors =>
      println("recieved Actors")
      current_actor_count += actors.actors.length
    /*  actors.SysId.scheduler.scheduleOnce(1 minutes , new Runnable {
      def run = {
        for(i <-  actors.actors){
        	i ! Kill;
        } 
        
        actors.SysId.shutdown() 
        println(mine_count )
      }
    })*/
    case actor: akka.actor.ActorRef =>
      println("Recieved an Actor")

    case stopd: AnActorStopped =>
      println("Sending Stopping Signal to Actor " + stopd.ActorId)
      stopd.actor ! new StopMessage(stopd.ActorId)
      current_actor_count -= 1
      if (current_actor_count == 0) {
        println("MINECOUNT = " + mine_count)
        println("TIME = " + reqTime + " mins")
        System.exit(0);
      }

    case remote_cnnt: remoteConnect =>
      println("A remote Actor has connected..")
      println("Assigning ID to it")
      current_actor_count += 1
      var rem_id = new remoteAssignID(current_actor_count, gatorId, zeroes, reqTime)
      sender ! rem_id

    case "testMessage" =>
      println("recieved test Message")
  }
}

class worker extends Actor {
  def receive = {
    case "Test" =>
      println("test recieved");
    case params: startWork =>
      println("Actor" + params.actor_id + " started")
      var count = System.nanoTime();
      var current_time = count;
      var end_time = current_time + (60 * params.req_time * 1000000000.0)
      while (count <= end_time) {
        var currentStr = params.GatorId + ";" + params.actor_id.toString() + Random.alphanumeric.take(5).mkString
        var hash = sha(currentStr);
        if (hash.startsWith(params.zeroes)) {
          //	  	    println(params.actor_id + " : " + currentStr + " : " + hash)
          val proc_msg = new processMsg(params.actor_id, currentStr, hash)
          params.controller ! proc_msg
        }
        count = System.nanoTime();
        //	  	  count-=1;
      }
      params.controller ! AnActorStopped(params.actor_id, self);
    case stop_msg: StopMessage =>
      println("Actor " + stop_msg.actor_id + "stopped")
      context.stop(self);
  }

  def bytes2hex(bytes: Array[Byte], sep: Option[String] = None): String = {
    sep match {
      case None => bytes.map("%02x".format(_)).mkString
      case _ => bytes.map("%02x".format(_)).mkString(sep.get)
    }
  }

  def sha(s: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    // return encoded value 
    return bytes2hex((md.digest(s.getBytes("UTF-8"))))
  }
}

class LocalActor extends Actor {

  // create the remote actor
  var ip = ProjectPart2.getIP()
  val remote = context.actorFor("akka.tcp://project1System@" + ip + ":2552/user/CONTROLLER_ACTOR")
  var counter = 0
  def receive = {
    case rmc: remoteActorCount =>
      counter = rmc.count
      println("Starting local actor")
      remote ! "testMessage"
      var rem_con = new remoteConnect
      remote ! rem_con
    case myid: remoteAssignID =>
      var myActorId = myid.id
      println("Actor" + myActorId + " started")
      var count = System.nanoTime();
      var current_time = count;
      var end_time = current_time + (60 * myid.req_time * 1000000000.0)
      while (count <= end_time) {
        var currentStr = myid.GatorId + ";" + myActorId.toString() + Random.alphanumeric.take(5).mkString
        var hash = sha(currentStr);
        if (hash.startsWith(myid.zeroes)) {
          //	  	    println(params.actor_id + " : " + currentStr + " : " + hash)
          val proc_msg = new processMsg(myid.id, currentStr, hash)
          sender ! proc_msg
        }
        count = System.nanoTime();
        //	  	  count-=1;
      }
      sender ! AnActorStopped(myid.id, self);
    case stop_msg: StopMessage =>
      println("Actor " + stop_msg.actor_id + "stopped")
      context.stop(self);
    case _ =>
      println("LocalActor got something unexpected.")
  }

  def bytes2hex(bytes: Array[Byte], sep: Option[String] = None): String = {
    sep match {
      case None => bytes.map("%02x".format(_)).mkString
      case _ => bytes.map("%02x".format(_)).mkString(sep.get)
    }
  }

  def sha(s: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    // return encoded value 
    return bytes2hex((md.digest(s.getBytes("UTF-8"))))
  }
}




