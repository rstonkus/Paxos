open Paxos.BasicPaxos
open System.Collections.Generic

type Acceptor = { mutable AState : AState }
type Proposer = { mutable PState : PState }
type Learner = { mutable LState : unit }


[<EntryPoint>]
let main argv = 
  let failwithF msg args = failwith (sprintf msg args)

  let acceptors = 
    [
      ("acceptor1", { AState = AReady 0});
      ("acceptor2", { AState = AReady 0});
      ("acceptor3", { AState = AReady 0})
    ]

  let quorumSize = 2
  let proposers = 
    [
      ("proposer1", { PState = PWaiting 0});
      ("proposer2", { PState = PWaiting 0})
    ]
  let learners =
    [
      ("learner1", {LState = ()});
      ("learner2", {LState = ()});
    ];
  let clients = 
    [
       ("client1", {LState = ()});
       ("client2", {LState = ()});
    ];
  
  let find xs d = 
    match List.tryFind (fun x -> fst x = d) xs with
    | None -> failwithF "no such destination %A" d
    | Some (_,p) -> p
    
  let wrapP (d,m) = (d,PMsg m)

  let allAcceptors = List.map fst acceptors

  let inputBuffer = Queue<Destination * Msg>()
  let outputBuffer = Queue<Destination * Msg>()

  let handleMsg d msg = 
    match (d,msg) with
    | (Proposer d, CMsg (MClientRequest cr)) ->
      let proposer = find proposers d
      let (s',out) = proposerReceiveFromClient proposer.PState (MClientRequest cr)
      let () = proposer.PState <- s'
      Option.map wrapP out |> Option.iter outputBuffer.Enqueue
    | (BroadcastAcceptors, PMsg (MPrepare (n,key))) ->
      
    | (Proposer d, AMsg (MPromise (n,vv))) -> ()
    | (BroadcastAcceptors, PMsg (MAccept (n,key,v))) -> ()
    | (BroadcastProposersLearners, AMsg m) -> ()
    | (Client d, LMsg (MResponse v)) -> ()
    | (Client d, PMsg (MClientNack (guid,reason))) -> ()
    | _ -> failwithF "bad message destination %A" (d,msg)
  
  

  //a simple mutator that always writes "v1"
  let writeV1 vvo = "v1"
  let writeV2 vvo = "v2"
  //read with default
  let readWithDefault d vvo = 
    match vvo with
    | None -> d
    | Some (_,v) -> v

  let req1 = CMsg (MClientRequest ((System.Guid.NewGuid(),"client1"),"key1",writeV1))
  let req2 = CMsg (MClientRequest ((System.Guid.NewGuid(),"client1"),"key1",readWithDefault "default"))

  let () = inputBuffer.Enqueue (Proposer "proposer1", req1)
  let () = inputBuffer.Enqueue (Proposer "proposer1", req2)

  let random = System.Random(1234);
  while (inputBuffer.Count <> 0 || outputBuffer.Count <> 0)
    do if (random.Next 3 = 0 && outputBuffer.Count <> 0)
       then let o = outputBuffer.Dequeue()
            inputBuffer.Enqueue o
       let (d,m) = inputBuffer.Dequeue()
       handleMsg d m


  printfn "%A" argv
  0 // return an integer exit code



  //  let proposerDest = List.map fst proposers
  //  let proposerDest = List.map fst learners

