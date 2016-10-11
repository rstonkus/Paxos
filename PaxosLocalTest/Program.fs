open Paxos.BasicPaxos
open Paxos.BasicPaxos.Test
open FsCheck.Xunit
open System

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
      Participant.external "external1";
      Participant.external "external2";
    |];
  in (quorumSize,participants)

let atMostOnce (f:Value option -> Value) (sender:Sender) (g:Guid) (vo:ValueLastTouched option) : ValueLastTouched =
  match vo with
  | None -> (f None,[(sender,g)])
  | Some (v,xs) -> 
    let xo = List.tryFind (fun (_,guid) -> guid = g) xs
    match xo with
    | None -> let xs' = List.filter (fst >> (<>) sender) xs
              in (f (Some v), (sender,g)::xs')
    | Some _ -> (v,xs) //same req, we leave it untouched

//a simple mutator that always writes "v1"
let writeV1 (vo:Value option) : Value = "v1"

//read with default
let readWithDefault d (vo:Value option) : Value = 
  match vo with
  | None -> d
  | Some v -> v

let increment (vo:Value option) : Value = 
  match vo with
  | None -> "1"
  | Some v -> 
    let i = System.Int32.Parse v
    in (i + 1).ToString()

let mutable count = 0
let appendCount (vo:Value option) : Value = 
  match vo with
  | None -> 
    let r = count.ToString()
    let () = count <- count + 1
    in r
  | Some v -> 
    let r = count.ToString()
    let () = count <- count + 1
    in v+","+r


[<Property(MaxTest = 100)>]
let ``single round write and readwithDefault`` (seed:int) = 
  let debug = false
  let (quorumSize, participants) = clusterOf3 ()
  let random = System.Random(seed)

  let result = { Run.Responses = [] }
  let external1 = Participant.findExternal "external1" participants
  let external2 = Participant.findExternal "external2" participants
  

  let guid1 = System.Guid.NewGuid()
  let req1 = (Proposer "proposer1", EMsg (MExternalRequest ("external1", ((guid1, "key1"), atMostOnce writeV1 "external1" guid1))))
  let () = Run.externalReceive external1 req1

  let guid2 = System.Guid.NewGuid()
  let req2 = (Proposer "proposer2", EMsg (MExternalRequest ("external2", ((guid2, "key2"), atMostOnce writeV1 "external2" guid1))))
  let () = Run.externalReceive external2 req2
  

  let () = Run.runToEnd debug random false quorumSize participants result

  //verify
  let v1s = Run.Verify.responsesToGuid guid1 result
  let v2s = Run.Verify.responsesToGuid guid2 result

  let (v1,_) = Seq.head v1s //there must be one anyway
  let (v2,_) = Seq.head v2s

  let allEq1 = Seq.forall (fst >> (=) v1) v1s
  let allEq2 = Seq.forall (fst >> (=) v2) v2s
  
  let always = v1 = "v1" && allEq1 && allEq2
  let case1 = v2 = "default"
  let case2 = v2 = "v1"
  always && (case1 || case2)


[<Property(MaxTest = 10)>]
let ``1000 increments by two clients`` (seed:int) = 
  let debug = false
  let (quorumSize, participants) = clusterOf3 ()
  let random = System.Random(seed)

  let result = { Run.Responses = [] }
  let external1 = Participant.findExternal "external1" participants
  let external2 = Participant.findExternal "external2" participants
  
  let twoIncs () = 
    let guid1 = System.Guid.NewGuid()
    let req1 = (Proposer "proposer1", EMsg (MExternalRequest ("external1", ((guid1, "key"), atMostOnce appendCount "external1" guid1))))
    let () = Run.externalReceive external1 req1

    let guid2 = System.Guid.NewGuid()
    let req2 = (Proposer "proposer2", EMsg (MExternalRequest ("external2", ((guid2, "key"), atMostOnce appendCount "external2" guid2))))
    let () = Run.externalReceive external2 req2
    ()
  
  let it = 10
  let bs = 
    seq {
      for i in 1 .. it -> 
        twoIncs ()
    }
  let () = Seq.iter id bs
  let () = Run.runToEnd debug random false quorumSize participants result
  let g = System.Guid.NewGuid()
  let r = (Proposer "proposer1", EMsg (MExternalRequest ("external1", ((g, "key"), atMostOnce (readWithDefault "d") "external1" g))))
  let () = Run.externalReceive external1 r
  let () = Run.runToEnd debug random false quorumSize participants result


  //verify
  let vs = Run.Verify.responsesToGuid g result
  let responses = result.Responses |> List.map (fun (_,_,LMsg (MResponse (g,v))) -> v) |> List.toArray
//  printfn "%A" participants
//  printfn "%A" result
  printf "."


  let v = Seq.head vs |> fst //there must be one anyway
  let allEq = Seq.forall (fst >> (=) v) vs
  
  let always = v = ((2*it).ToString ()) && allEq
  always
  


[<EntryPoint>]
let main argv = 
  let b = ``1000 increments by two clients`` 59833166
  printf "%b" b
  
//  let rSeed = System.Random()
//  let seed = rSeed.Next ()
//  printfn "%i" seed
//  let r = System.Random(seed)
//  
//  let bs = 
//    seq {
//      for i in 1 .. 100 -> 
//        let seed' = r.Next ()
//        //let () = printfn "%i" seed'
//        in (seed',``1000 increments by two clients`` seed')
//    }
//  let s (i,b) = (sprintf "(%b,%i)" b i)
//  printfn "%s" (System.String.Join (",\n", bs |> Seq.filter (fun (seed,b) -> not b) |> Seq.map s))
  0 // return an integer exit code


