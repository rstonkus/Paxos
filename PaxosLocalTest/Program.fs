open Paxos.BasicPaxos
open Paxos.BasicPaxos.Test
open System.Collections.Generic

[<EntryPoint>]
let main argv = 
  let debug = false
  let random = System.Random(1234)
  let quorumSize = 2
  let participants = 
    [
      Participant.Proposer { Name = "proposer1"; PState = PReady 0;             Output = Queue<Destination*Msg>(); Input = Queue<string * Msg>() };
      //Participant.Proposer { Name = "proposer2"; PState = PReady 0;             Output = Queue<Destination*Msg>(); Input = Queue<string * Msg>() };
      Participant.Acceptor { Name = "acceptor1"; AState = AReady (0,Map.empty); Output = Queue<Destination*Msg>(); Input = Queue<string * Msg>() };
      Participant.Acceptor { Name = "acceptor2"; AState = AReady (0,Map.empty); Output = Queue<Destination*Msg>(); Input = Queue<string * Msg>() };
      Participant.Acceptor { Name = "acceptor3"; AState = AReady (0,Map.empty); Output = Queue<Destination*Msg>(); Input = Queue<string * Msg>() };
      Participant.Learner  { Name = "learner1";  LState = LReady Map.empty;     Output = Queue<Destination*Msg>(); Input = Queue<string * Msg>() };
      Participant.Learner  { Name = "learner2";  LState = LReady Map.empty;     Output = Queue<Destination*Msg>(); Input = Queue<string * Msg>() };
      Participant.Client   { Name = "client1";                                  Output = Queue<Destination*Msg>(); Input = Queue<string * Msg>() };
      Participant.Client   { Name = "client2";                                  Output = Queue<Destination*Msg>(); Input = Queue<string * Msg>() };
    ];
  
  //a simple mutator that always writes "v1"
  let writeV1 vvo = "v1"
  let writeV2 vvo = "v2"

  //read with default
  let readWithDefault d vvo = 
    match vvo with
    | None -> d
    | Some (_,v) -> v

  let req1 = (Proposer "proposer1", CMsg (MClientRequest ((System.Guid.NewGuid(),"client1"),"key1", writeV1)))
  let () = Run.sendMsg participants "client1" req1
  let () = Run.go debug random quorumSize participants

  let req2 = (Proposer "proposer1", CMsg (MClientRequest ((System.Guid.NewGuid(),"client1"),"key1", readWithDefault "default")))
  let () = Run.sendMsg participants "client1" req2
  let () = Run.go debug random quorumSize participants



  let client1 = Participant.findC "client1" participants
  let client1responses = Run.drainQueue client1.Input

  printfn "%A" client1responses
  0 // return an integer exit code


