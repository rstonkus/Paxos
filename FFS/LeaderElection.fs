namespace Paxos.LeaderElection

open System
open System.Threading
open Paxos
open Paxos.BasicPaxos
open Paxos.LeaderElection.ZMQ
open Paxos.LeaderElection

module LeaderElection =
  type Config =
    {
      Me : string
      MeEndpoint : string
      Endpoint1 : string
      Endpoint2 : string
      LeadershipTimeout : TimeSpan
    }
  type CurrentLeader(timeout : TimeSpan, leaderChanged : string option -> unit) =
    let mutable lastLeader = None
    let mutable lastTime = DateTime.Now
    let confirmLeader () =
      lastTime <- DateTime.Now
    let updateLeader l = 
      if l <> lastLeader
      then lastLeader <- l
           lastTime <- DateTime.Now
           leaderChanged(lastLeader)
      else confirmLeader ()
    
    member this.LeaderIs(newLeader : string) =
      updateLeader (Some newLeader)

    member this.CheckTimeout() =
      let span = DateTime.Now - lastTime
      if span > timeout
      then updateLeader None

  let loop (config:Config) (cancel:CancellationToken) leaderUpdated =
    //Conventions 
    let leaderKey = "key"
    let leaderElectionOperation =
      Operations.idempotent (Operations.leaderElection config.Me 5)
    let leaderFromDbString (s:string) =
      (s.Split ',').[0]

    let freshRequest () =
      let g = Guid.NewGuid()
      let op = leaderElectionOperation config.Me g
      let session = (g,leaderKey)
      in (session,op)


    //Stitching together
    let currentLeader = CurrentLeader(config.LeadershipTimeout, leaderUpdated)
    use zmq = new ZMQTransport(config.Me,config.MeEndpoint,config.Endpoint1,config.Endpoint2)
  
    let response (v:Value) =
      match v with
      | None ->
        failwith "Got None from paxos. Should never happen with this operator"
      | Some s ->
        let leader = leaderFromDbString s
        currentLeader.LeaderIs leader

    let n = new Cluster.Node(config.Me, Cluster.standardClusterOf3, zmq.Send, response)

    let mutable lastLeaderElect = DateTime.Now //will postpone the first request a little
    let request time = 
      let eReq = freshRequest()
      let () = lastLeaderElect <- time
      n.Request eReq
          
    //Main loop
    while not cancel.IsCancellationRequested do
      //check if leadership has timed out
      currentLeader.CheckTimeout()

      //check if request for leadership has timed out
      n.CheckTimeout()

      //handle local messages
      n.HandleLocalMessage()

      //receive now messages
      zmq.Receive (fun (sender,d,m) -> n.Consume sender d m)
    
      //check for dead zmq subscriber sockets (known bug in ZMQ)
      zmq.CheckConnections()

      //initiate new leaderelection/confirmation in case we don't already have one active.
      if not (n.HasRequestInProgress())
      then let now = DateTime.Now
           if now - lastLeaderElect > TimeSpan.FromSeconds 1.0
           then request now

      //Give the cpu a little break
      Thread.Sleep 1

