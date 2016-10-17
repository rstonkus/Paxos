namespace Paxos.LeaderElection
//
//open Paxos.BasicPaxos
//
//
//module LeaderElection = 
//  type Node(quorumSize:int, proposerCount:int) =
//    let mutable pState = freshPState
//    let mutable cState = freshCState
//    let mutable aState = freshAState
//    let mutable lState = freshLState
//
//    member val QuorumSize = quorumSize
//    member val ProposerCount = proposerCount
//  
//    member this.Send (s : Destination * Msg) = ()
//
//    member this.Consume d msg = 
//      let now = System.DateTime.Now.Ticks
//      let pReceiveFromE m = 
//        let (cs',ps',o) = proposerReceiveFromExternal cState pState now m
//        pState <- ps'
//        cState <- cs'
//        o |> Option.iter (wrapP >> this.Send)
//      let aReceiveFromP m =
//        let (as',o) = acceptorReceiveFromProposer aState m sender 
//        aState <- as'
//        o |> Option.iter (wrapA >> this.Send)
//      let pReceiveFromA m =
//        let (cs',ps',o) = proposerReceiveFromAcceptor quorumSize now cState pState m
//        pState <- ps'
//        cState <- cs'
//        o |> Option.iter (wrapP >> this.Send)
//      let lReceiveFromA m =
//        let (ls', o) = learnerReceiveFromAcceptor lState m sender quorumSize
//        lState <- ls'
//        o |> Option.iter (wrapL >> this.Send)
//      let aReceiveFromA m =
//        let (as',o) = acceptorReceiveFromAcceptor proposerCount quorumSize aState m sender
//        aState <- as'
//        o |> Option.iter (wrapA >> this.Send)
//
//      match (d,msg) with
//      | (Proposer _, (EMsg (MExternalRequest _ as m))) -> pReceiveFromE m
//      | (BroadcastAcceptors, (PMsg (MPrepare _ as m))) -> aReceiveFromP m
//      | (Proposer _, (AMsg (MPromise _ as m))) -> pReceiveFromA m
//      | (BroadcastAcceptors, (PMsg (MAccept _ as m))) -> aReceiveFromP m
//      | (Proposer _, (AMsg (MNackPrepare _ as m))) -> pReceiveFromA m
//      | (Broadcast, (AMsg (MAccepted _ as m))) -> 
//        pReceiveFromA m
//        aReceiveFromA m
//        lReceiveFromA m
//        
//
//      | (External _, (LMsg (MResponse _) as m)) ->
//        send msg d
//      | (BroadcastAcceptors, (AMsg (MSyncRequest _) as m)) -> 
//        broadcastA msg
//      | (Acceptor _, (AMsg (MSyncResponse _) as m)) -> 
//        send msg d
//      | _ -> failwithf "bad message destination %A" msg
//
//
////    member this.Send msg =
////      match msg with
////      | (Proposer d, (EMsg (MExternalRequest _) as msg)) -> send msg d
////      | (BroadcastAcceptors, (PMsg (MPrepare _) as msg)) -> broadcastA msg
////      | (Proposer d, (AMsg (MPromise _) as msg)) -> send msg d
////      | (BroadcastAcceptors, (PMsg (MAccept _) as msg)) -> broadcastA msg
////      | (Proposer d, (AMsg (MNackPrepare _) as msg)) -> send msg d
////      | (Broadcast, (AMsg (MAccepted _) as msg)) -> broadcast msg
////      | (External d, (LMsg (MResponse _) as msg)) -> send msg d
////      | (BroadcastAcceptors, (AMsg (MSyncRequest _) as msg)) -> broadcastA msg
////      | (Acceptor d, (AMsg (MSyncResponse _) as msg)) -> send msg d
////      | _ -> failwithf "bad message destination %A" m
//      
//
////  let sendMsg participants sender m = 
////    let send msg d = Participant.sendToDest (sender,msg) d participants
////    let broadcast msg = Participant.broadcast participants (sender,msg)
////    let broadcastA msg = Participant.broadcastA participants (sender,msg)
////    match m with
////    | (Proposer d, (EMsg (MExternalRequest _) as msg)) -> send msg d
////    | (BroadcastAcceptors, (PMsg (MPrepare _) as msg)) -> broadcastA msg
////    | (Proposer d, (AMsg (MPromise _) as msg)) -> send msg d
////    | (BroadcastAcceptors, (PMsg (MAccept _) as msg)) -> broadcastA msg
////    | (Proposer d, (AMsg (MNackPrepare _) as msg)) -> send msg d
////    | (Broadcast, (AMsg (MAccepted _) as msg)) -> broadcast msg
////    | (External d, (LMsg (MResponse _) as msg)) -> send msg d
////    | (BroadcastAcceptors, (AMsg (MSyncRequest _) as msg)) -> broadcastA msg
////    | (Acceptor d, (AMsg (MSyncResponse _) as msg)) -> send msg d
////    | _ -> failwithf "bad message destination %A" m
