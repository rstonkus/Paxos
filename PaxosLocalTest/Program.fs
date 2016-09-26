open Paxos.BasicPaxos
open Paxos.BasicPaxos.Test
open System.Collections.Generic

[<EntryPoint>]
let main argv = 
  let debug = false
  let seed = System.Random().Next System.Int32.MaxValue
  //let seed = 1204047459
  let () = printfn "%i" seed
  let random = System.Random(seed)//1204047459 -> default
  let quorumSize = 2
  let participants = 
    [|
      Participant.proposer "proposer1"; 
      Participant.proposer "proposer2"; 
      Participant.acceptor "acceptor1"; 
      Participant.acceptor "acceptor2"; 
      Participant.acceptor "acceptor3"; 
      Participant.learner  "learner1";
      Participant.learner  "learner2";
      Participant.client   "client1";
      Participant.client   "client2";
    |];
  
  //a simple mutator that always writes "v1"
  let writeV1 vvo = "v1"
  let writeV2 vvo = "v2"

  //read with default
  let readWithDefault d vvo = 
    match vvo with
    | None -> d
    | Some (_,v) -> v

  let result = { Run.Responses = [] }
  let client1 = Participant.findClient "client1" participants
  let client2 = Participant.findClient "client2" participants
  
  let req1 = (Proposer "proposer1", CMsg (MClientRequest ((System.Guid.NewGuid(),"client1"),"key1", writeV1)))
  let () = Run.clientSend client1 req1
  //let () = Run.runToEnd debug random quorumSize participants result 

  let req2 = (Proposer "proposer2", CMsg (MClientRequest ((System.Guid.NewGuid(),"client2"),"key1", readWithDefault "default")))
  let () = Run.clientSend client2 req2
  let () = Run.runToEnd debug random quorumSize participants result

    
  printfn "%A" result.Responses
  0 // return an integer exit code


