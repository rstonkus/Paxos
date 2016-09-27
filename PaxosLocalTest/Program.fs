﻿open Paxos.BasicPaxos
open Paxos.BasicPaxos.Test
open FsCheck.Xunit

let clusterOf3 () =
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
  in (quorumSize,participants)

[<Property(MaxTest = 500)>]
let ``single round write and readwithDefault`` (seed:int) = 
  let debug = false
  let (quorumSize, participants) = clusterOf3 ()
  let random = System.Random(seed)//1204047459 -> default

  //a simple mutator that always writes "v1"
  let writeV1 vvo = "v1"

  //read with default
  let readWithDefault d vvo = 
    match vvo with
    | None -> d
    | Some (_,v) -> v

  let result = { Run.Responses = [] }
  let client1 = Participant.findClient "client1" participants
  let client2 = Participant.findClient "client2" participants
  


  let guid1 = System.Guid.NewGuid()
  let req1 = (Proposer "proposer1", CMsg (MClientRequest ((guid1,"client1"),"key1", writeV1)))
  let () = Run.clientSend client1 req1

  let guid2 = System.Guid.NewGuid()
  let req2 = (Proposer "proposer2", CMsg (MClientRequest ((guid2,"client2"),"key1", readWithDefault "default")))
  let () = Run.clientSend client2 req2
  let () = Run.runToEnd debug random quorumSize participants result



  //verify
  let v1s = Run.Verify.responsesToGuid guid1 result
  let v2s = Run.Verify.responsesToGuid guid2 result

  let v1 = Seq.head v1s //there must be one anyway
  let v2 = Seq.head v2s

  let allEq1 = Seq.forall ((=) v1) v1s
  let allEq2 = Seq.forall ((=) v2) v2s
  
  //let () = printfn "%A" result.Responses

  let always = v1 = "v1" && allEq1 && allEq2
  let case1 = v2 = "default"
  let case2 = v2 = "v1"
  always && (case1 || case2)

  


[<EntryPoint>]
let main argv = 
  let rSeed = System.Random()
  let seed = rSeed.Next ()
  printfn "%i" seed
  let r = System.Random(seed)
  //let seed = 1204047459
  

  let bs = 
    seq {
      for i in 1 .. 10000 -> 
        let seed' = r.Next ()
        in ``single round write and readwithDefault`` seed'
    }
   
  printfn "%A" (Seq.forall id bs)
  0 // return an integer exit code


