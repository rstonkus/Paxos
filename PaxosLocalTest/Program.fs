open Paxos.BasicPaxos
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

//a simple mutator that always writes "v1"
let writeV1 vvo = "v1"

//read with default
let readWithDefault d vvo = 
  match vvo with
  | None -> d
  | Some (_,v) -> v

let increment vvo = 
  match vvo with
  | None -> "1"
  | Some (_,v) -> let i = System.Int32.Parse v
                  in (i + 1).ToString()


[<Property(MaxTest = 100)>]
let ``single round write and readwithDefault`` (seed:int) = 
  let debug = false
  let (quorumSize, participants) = clusterOf3 ()
  let random = System.Random(seed)

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
  
  let always = v1 = "v1" && allEq1 && allEq2
  let case1 = v2 = "default"
  let case2 = v2 = "v1"
  always && (case1 || case2)


[<Property(MaxTest = 100)>]
let ``1000 increments by two clients`` (seed:int) = 
  let debug = true
  let (quorumSize, participants) = clusterOf3 ()
  let random = System.Random(seed)

  let result = { Run.Responses = [] }
  let client1 = Participant.findClient "client1" participants
  let client2 = Participant.findClient "client2" participants
  
  let twoIncs () = 
    let guid1 = System.Guid.NewGuid()
    let req1 = (Proposer "proposer1", CMsg (MClientRequest ((guid1,"client1"),"key1", increment)))
    let () = Run.clientSend client1 req1

    let guid2 = System.Guid.NewGuid()
    let req2 = (Proposer "proposer2", CMsg (MClientRequest ((guid2,"client2"),"key1", increment)))
    let () = Run.clientSend client2 req2
    ()
  
  let it = 20
  let bs = 
    seq {
      for i in 1 .. it -> 
        twoIncs ()
    }
  let () = Seq.iter id bs

  let () = Run.runToEnd debug random quorumSize participants result

  let g = System.Guid.NewGuid()
  let r = (Proposer "proposer1", CMsg (MClientRequest ((g,"client1"),"key1", readWithDefault "default")))
  let () = Run.clientSend client1 r
  let () = Run.runToEnd debug random quorumSize participants result


  //verify
  let vs = Run.Verify.responsesToGuid g result
//  printfn "%A" participants
//  printfn "%A" result
  printf "."


  let v = Seq.head vs //there must be one anyway
  let allEq = Seq.forall ((=) v) vs
  
  let always = v = (it.ToString ()) && allEq
  always
  


[<EntryPoint>]
let main argv = 
  let b = ``1000 increments by two clients`` 2033103372
  printf "%b" b

//  let rSeed = System.Random()
//  //let seed = rSeed.Next ()
//  let seed = 1234
//  printfn "%i" seed
//  let r = System.Random(seed)
//  
//  let bs = 
//    seq {
//      for i in 1 .. 1000 -> 
//        let seed' = r.Next ()
////        let () = printfn "%i" seed'
//        in (seed',``1000 increments by two clients`` seed')
//    }
//  let s (i,b) = (sprintf "(%b,%i)" b i)
//  printfn "%s" (System.String.Join (",\n", bs |> Seq.map s))
  0 // return an integer exit code


