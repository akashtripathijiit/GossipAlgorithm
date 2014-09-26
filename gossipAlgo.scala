import akka.actor._
import akka.pattern._
import akka.util.Timeout
import scala.concurrent.duration._
import akka.dispatch.Foreach
//import system.dispatcher

  sealed trait GossipPushSum
  case object Intitiate extends GossipPushSum

object gossipAlgo {

  
  def main(args: Array[String]) : Unit = {
    if(args.length != 3)
    	println("Invalid number of arguments")
    else
    {
    	val system = ActorSystem("gossipMainSystem")    
    	val master = system.actorOf(Props(new Master(args(0).toInt, args(1).toLowerCase(), args(2).toLowerCase())), "Master")
    	master ! "start"
    }
  }
}

// MASTER CLASS
class Master(numOfWorkers: Int, topology: String, typeOfAlgorithm: String) extends Actor
{
     var worker : ActorRef = _
     var aliveWorkers = numOfWorkers
     val system = context.system
     
     
     var deadNodes:List[Int] = List.empty
     var timeStart:Long = 0;
    
     // CREATING WORKERS HERE AS PER REQUIRED ALGORITHM
    
     if (typeOfAlgorithm == "push-sum")
     {
    	 for( i <- 0 until numOfWorkers )
         	 worker =  context.system.actorOf(Props(new PushSumWorker(numOfWorkers, topology)), "" + i.toString)
     }  
     else if (typeOfAlgorithm == "gossip")
     {	
    	 for( i <- 0 until numOfWorkers )
    		 worker =  context.system.actorOf(Props(new GossipWorker(numOfWorkers, topology)), "" + i.toString)
     }
     //Thread.sleep(100);
     
     def receive = {
       case "start" =>
      
     if(typeOfAlgorithm == "push-sum")
         worker ! List(0.0, 0.0)
     else if(typeOfAlgorithm == "gossip")
         worker ! "rumor"
     else 
         println("Invlaid type of algorithm")
     
 
     case "exiting" => 
        aliveWorkers = aliveWorkers - 1
        //println("active workers = " + aliveWorkers)
        if(aliveWorkers == 1){
        	sender ! "stop"
        	context.system.shutdown()
       }
       /*
       case "shutdown" => println("timeout")
       context.system.shutdown()*/
     }
     
     
     //TIMING MECHANISM
     override def preStart() = 
     {
        timeStart = System .currentTimeMillis()
     }
     override def postStop()
      {
       println("Time taken is "+ (System.currentTimeMillis() - timeStart)+" msecs")  
      }
     
}
 
// GOSSIP WORKER CLASS DEFINITION
 class GossipWorker(numNodes: Int, topology: String) extends Actor
 {
  var activeWorkers : Array[Int] = _
  var gossipCount: Int = 0
  var selfWorkerNumber : Int = _ 
  var neighbors : Array[Int] = _
  var numberOfNeighbors : Int = _
  var schedulerHolder:Cancellable = null;
  
  //temporary variables for testing
  var gossipSent : Int = _
  
  override def preStart()
  {
    //Thread.sleep(100)
    activeWorkers = new Array[Int](numNodes)
	 var z : Int = 0
    for(i <- 0 until numNodes)
	  {  
	     activeWorkers(z) = i
	     z += 1
	  }
	  activeWorkers.indexOf(5)
    selfWorkerNumber = self.path.name.toInt;
	if(topology == "full")
	{
	  numberOfNeighbors = numNodes - 1
	  neighbors = new Array[Int](numberOfNeighbors)
	  var ctr : Int = 0
	  for(i <- 0 until numNodes)
	  {  
	     if(self.path.name != i.toString)
	     {	 
	     	 neighbors(ctr) = i
	     	 ctr += 1
	     }
	  }
	  /*for(n <- neighbors)
	  {
		  print(self.path.name + ":" + n + " ")
	  }*/
	 }
	else if(topology == "line")
	{
	  setLineTopology()
	  
	}
	
  }
  
  def setLineTopology()
  {
    var selfIndex : Int = activeWorkers.indexOf(selfWorkerNumber)
	  if (activeWorkers.length == 1)
	  {}//println("very small number of nodes")
	  else if(selfIndex == 0)
	  {  
	    numberOfNeighbors = 1
	    neighbors = new Array[Int](1)
	    neighbors(0) = activeWorkers(selfIndex+1)
//	println("+"+ selfWorkerNumber)
//	for(n<-neighbors)
//	  print(n + "+")
//	println()
	  }
	  else if(selfIndex == activeWorkers.length - 1)
	  { 
	    numberOfNeighbors = 1
	    neighbors = new Array[Int](1)
	    neighbors(0) = activeWorkers(selfIndex-1)
//	  println("-" + selfWorkerNumber)
//	for(n<-neighbors)
//	  print(n + "-")
//	println()
	  }
	  else
	  {
	    //println("my index = " + selfIndex)
	    numberOfNeighbors = 2
	    neighbors = new Array[Int](2)
	    neighbors(0) = activeWorkers(selfIndex - 1)
	    neighbors(1) = activeWorkers(selfIndex + 1)
	  //print("//" + selfWorkerNumber)
	//for(n<-neighbors)
	 // print(n + "/")
	//println()
	  }
  }
  
  def receive = {
    case "rumor"  =>
      if(gossipCount == 0)
      {
        //import system.dispacther;
        import scala.concurrent.ExecutionContext.Implicits.global
        schedulerHolder =  context.system.scheduler.schedule(0 seconds, 8 milliseconds, self, "send")
      }
      gossipCount += 1
      if(gossipCount == 10)
      { 
        schedulerHolder.cancel
        //println(self.path.name +  " : i wont send any messages")
        context.actorSelection("../Master") ! "exiting"
      }
      if(gossipCount >= 10)
      {
        sender ! "iAmExitting"
        
      }
    case "send" =>
      //println(self.path.name + " length = "+ neighbors.length)
      var neighborToSend : ActorSelection = selectRandomWorker()
      neighborToSend ! "rumor"
      
    case "iAmExitting" =>
      //println("removing " + sender.path.name)
     	if(neighbors.contains(sender.path.name.toInt))
     		removeNeighborFromList(sender.path.name);
      
      
    case "stop" =>
    	schedulerHolder.cancel()
      
  }
  
  def removeNeighborFromList(nameOfActor: String) =
  {
    numberOfNeighbors -= 1
    var newNeighbors = new Array[Int](numberOfNeighbors)
    var i = 0
    
    //println("")
    for(n <- neighbors)
    {
      if(nameOfActor != n.toString)
      { 
        newNeighbors(i) = n
         i +=1
      }
    }
    neighbors = newNeighbors
    if(neighbors.length == 0)
    {
      //println(selfWorkerNumber + " Out of neighbors")
    }
    
    if((topology != "full") && (numberOfNeighbors == 0))
    {
    	if(topology == "line")
    	{
    	  var newActiveWorkers = new Array[Int](activeWorkers.length-1)
    	  for(n <- activeWorkers)
    	  {
    		  if(nameOfActor != n.toString)
    		  { 
    			  newActiveWorkers(i) = n
    					  i +=1
    		  }
    	  }
    	  activeWorkers = newActiveWorkers
    	  setLineTopology()
    	}  
    }
//    print(self.path.name + "->")
//    for(n <- neighbors)
//    	print(n + ",")
//    println()
    
  }
  
  def selectRandomWorker() : ActorSelection = 
  {
    var x : Int = math.floor((math.random * numberOfNeighbors)).toInt
    var result : ActorSelection = context.actorSelection("../"+neighbors(x).toString)
    //println(self.path.name + " -> " + neighbors(x).toString)
    result
  }
}

 // PUSH-SUM WORKER CLASS
class PushSumWorker(numNodes: Int, topology: String) extends Actor
{
	def receive =
	{
	  case _ =>
	}
}


