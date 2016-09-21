namespace Paxos.BasicPaxos.Test

open Paxos.BasicPaxos
open Paxos.BasicPaxos.Test

module Run =
  //puts the msg in the right input queue
  let sendMsg participants sender m = 
    let sendP msg d = Participant.iterP d participants (fun x -> x.Input.Enqueue (sender,msg))
    let sendC msg d = Participant.iterC d participants (fun x -> x.Input.Enqueue (sender,msg))
    let broadcastA msg = Participant.iterAllA participants (fun x -> x.Input.Enqueue (sender,msg))
    let broadcastPL msg = 
      do Participant.iterAllP participants (fun x -> x.Input.Enqueue (sender,msg))
         Participant.iterAllL participants (fun x -> x.Input.Enqueue (sender,msg))
    match m with
    | (Proposer d, (CMsg (MClientRequest _) as msg)) -> sendP msg d
    | (BroadcastAcceptors, (PMsg (MPrepare _) as msg)) -> broadcastA msg
    | (Proposer d, (AMsg (MPromise _) as msg)) -> sendP msg d
    | (BroadcastAcceptors, (PMsg (MAccept _) as msg)) -> broadcastA msg
    | (BroadcastProposersLearners, (AMsg (MAccepted _) as msg)) -> broadcastPL msg
    | (Client d, (LMsg (MResponse _) as msg)) -> sendC msg d
    | (Client d, (PMsg (MClientNack _) as msg)) -> sendC msg d
    | _ -> failwithf "bad message destination %A" m
  
  let wrapA (d,m) = (d,AMsg m)
  let wrapP (d,m) = (d,PMsg m)
  let wrapL (d,m) = (d,LMsg m)
  let wrapC (d,m) = (d,CMsg m)

  let consumeMsg quorumSize participants (p:Participant.Participant) =
    match p with
    | Participant.Acceptor a ->
      let (sender, msg) = a.Input.Dequeue ()
      in match msg with
         | PMsg pmsg -> let (s',store',outMsgO) = acceptorReceiveFromProposer a.AState pmsg sender a.Store
                        do outMsgO |> Option.map wrapA |> Option.iter (sendMsg participants a.Name)
                           a.Store <- store'
                           a.AState <- s'
         | _ -> failwithf "Acceptor received unexpected message %A" msg
    | Participant.Proposer p ->
      let (sender, msg) = p.Input.Dequeue ()
      in match msg with
         | CMsg cmsg -> let (s',outMsgO) = proposerReceiveFromClient p.PState cmsg
                        do outMsgO |> Option.map wrapP |> Option.iter (sendMsg participants p.Name)
                           p.PState <- s'
         | AMsg amsg -> let (s',outMsgO) = proposerReceiveFromAcceptor quorumSize p.PState amsg
                        do outMsgO |> Option.map wrapP |> Option.iter (sendMsg participants p.Name)
                           p.PState <- s'
         | _ -> failwithf "Proposer received unexpected message %A" msg
    | Participant.Learner l -> 
      () //TODO
    | Participant.Client c -> ()

  let unfinishedParticipants ps =
    let canSend =
      ps
      |> Seq.filter (fun p -> (Participant.output p).Count <> 0)
      |> Seq.toArray
    let canHandle =
      ps
      |> Seq.filter (Participant.isClient >> not)
      |> Seq.filter (fun p -> (Participant.input p).Count <> 0)
      |> Seq.toArray
    in (canHandle,canSend)

  let go (random:System.Random) quorumSize participants = 
    let pickRandom (r:System.Random) xs =
      let length = Array.length xs
      let i = r.Next length
      in xs.[i]
    let mutable unfinished = unfinishedParticipants participants //not counting client inputs
    let mutable c = true
    while (c)
      do
        let (canHandle,canSend) = unfinished
        match random.Next 2 with
        //consume message
        | 0 -> if (Array.isEmpty canHandle)
               then ()
               else let participant = pickRandom random canHandle
                    consumeMsg quorumSize participants participant
        //send message
        | _ -> if (Array.isEmpty canSend)
               then ()
               else let participant = pickRandom random canSend
                    let name = Participant.name participant
                    let outBuf = Participant.output participant
                    sendMsg participants name (outBuf.Dequeue ())
        unfinished <- unfinishedParticipants participants
        c <- not (Array.isEmpty canHandle) || not (Array.isEmpty canSend)
  
  let drainQueue (buf:Queue<'a>) = 
    let mutable l = []
    while buf.Count <> 0
      do l <- buf.Dequeue :: l
    l

