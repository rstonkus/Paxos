namespace Paxos.BasicPaxos.Test

open Paxos.BasicPaxos
open Paxos.BasicPaxos.Test
open System.Collections.Generic


//Ideas for testing
// - Reset nodes
// - Node outage
module Run =
  type Result =
    {
      mutable Responses : (string * Sender * Msg) list 
    }

  let clientSend (client:Participant.Client) m =
    client.Output.Enqueue m

  //puts the msg in the right input queue
  let sendMsg participants sender m = 
    let send msg d = Participant.sendToDest (sender,msg) d participants
    let broadcast msg = Participant.broadcast participants (sender,msg)
    let broadcastA msg = Participant.broadcastA participants (sender,msg)
    match m with
    | (Proposer d, (CMsg (MClientRequest _) as msg)) -> send msg d
    | (BroadcastAcceptors, (PMsg (MPrepare _) as msg)) -> broadcastA msg
    | (Proposer d, (AMsg (MPromise _) as msg)) -> send msg d
    | (BroadcastAcceptors, (PMsg (MAccept _) as msg)) -> broadcastA msg
    | (Proposer d, (AMsg (MNackPrepare _) as msg)) -> send msg d
    | (Broadcast, (AMsg (MAccepted _) as msg)) -> broadcast msg
    | (Client d, (LMsg (MResponse _) as msg)) -> send msg d
    | (Client d, (PMsg (MClientNack _) as msg)) -> send msg d
    | _ -> failwithf "bad message destination %A" m
  
  let wrapA (d,m) = (d,AMsg m)
  let wrapP (d,m) = (d,PMsg m)
  let wrapL (d,m) = (d,LMsg m)
  let wrapC (d,m) = (d,CMsg m)

  let consumeMsg debug quorumSize participants result (p:Participant.Participant) =
    match p with
    | Participant.Acceptor a ->
      let (sender, msg) = a.Input.Dequeue ()
      let () = if debug then printfn "%s <- %A state: %A" a.Name msg a.AState
      let r = 
        match msg with
        | PMsg pmsg -> let (s',outMsgO) = acceptorReceiveFromProposer a.AState pmsg sender
                       do outMsgO |> Option.map wrapA |> Option.iter (sendMsg participants a.Name)
                          a.AState <- s'
        | AMsg amsg -> let (s',outMsgO) = acceptorReceiveFromAcceptor a.AState amsg sender
                       do outMsgO |> Option.map wrapA |> Option.iter (sendMsg participants a.Name)
                          a.AState <- s'
        | _ -> failwithf "Acceptor received unexpected message %A" msg
      do if debug then printfn "--> %A" a.AState
         r
    | Participant.Proposer p ->
      let (sender, msg) = p.Input.Dequeue ()
      let () = if debug then printfn "%s <- %A state: %A" p.Name msg p.PState
      let r = 
        match msg with
        | CMsg cmsg -> let (s',outMsgO) = proposerReceiveFromClient p.PState cmsg
                       do outMsgO |> Option.map wrapP |> Option.iter (sendMsg participants p.Name)
                          p.PState <- s'
        | AMsg amsg -> let (s',outMsgO) = proposerReceiveFromAcceptor quorumSize p.PState amsg
                       do outMsgO |> Option.map wrapP |> Option.iter (sendMsg participants p.Name)
                          p.PState <- s'
        | _ -> failwithf "Proposer received unexpected message %A" msg
      do if debug then printfn "--> %A" p.PState
         r
    | Participant.Learner l ->
      let (sender, msg) = l.Input.Dequeue ()
      let () = if debug then printfn "%s <- %A state: %A" l.Name msg l.LState
      let r = 
        match msg with
        | AMsg amsg -> let (s', outMsgO) = learnerReceiveFromAcceptor l.LState amsg sender quorumSize
                       do outMsgO |> Option.map wrapL |> Option.iter (sendMsg participants l.Name)
                          l.LState <- s'
        | _ -> failwithf "Learner received unexpected message %A" msg
      do if debug then printfn "--> %A" l.LState
         r
    | Participant.Client c ->
      let (sender, msg) = c.Input.Dequeue ()
      result.Responses <- (c.Name, sender, msg) :: result.Responses

  let unfinishedParticipants ps =
    let canSend =
      ps
      |> Seq.filter (fun p -> (Participant.output p).Count <> 0)
      |> Seq.filter (Participant.isCrashed >> not)
      |> Seq.toArray
    let canHandle =
      ps
      |> Seq.filter (Participant.isCrashed >> not)
      |> Seq.filter (fun p -> (Participant.input p).Count <> 0)
      |> Seq.toArray
    in (canHandle,canSend)

  let runToEnd (debug:bool) (random:System.Random) quorumSize participants result = 
    let pickRandom (r:System.Random) xs =
      let length = Array.length xs
      let i = r.Next length
      in xs.[i]
    let consume = consumeMsg debug quorumSize participants result
    let send = sendMsg participants
    let mutable unfinished = unfinishedParticipants participants //not counting client inputs
    let mutable c = true
    while (c)
      do
        let p1 = Participant.find "proposer1" participants
        let p2 = Participant.find "proposer2" participants
        let (canHandle,canSend) = unfinished
        match random.Next 3 with
        //consume message
        | 0 -> if (Array.isEmpty canHandle)
               then ()
               else let participant = pickRandom random canHandle
                    consume participant 
        //send message
        | 1 -> if (Array.isEmpty canSend)
               then ()
               else let participant = pickRandom random canSend
                    let name = Participant.name participant
                    let outBuf = Participant.output participant
                    send name (outBuf.Dequeue ())
        | _ (*2*) -> //maybe do something evil
          if (random.Next 20 <> 0) 
          then () // P(evil) = 1/3 * 1/20 = 1/60
          else match random.Next 2 with
               //crash an acceptor for up to 100 rounds
               | 0 -> let all =  Participant.allA participants
                      let alive = all |> Seq.filter (fun a -> a.CrashedFor = 0) |> Seq.toArray
                      if(Seq.length alive <= quorumSize) 
                      then () //we cannot crash more according to assumptions
                      else let toCrash = pickRandom random alive
                           let rounds = random.Next 100
                           printfn "XXX Crashing %s for %i rounds" toCrash.Name rounds
                           toCrash.Input.Clear()
                           toCrash.CrashedFor <- rounds
               
               //reset state
               | _ -> ()
//                 let p = pickRandom random participants
//                 let q =  Participant.input p
//                 let toDrop = random.Next (System.Math.Min (q.Count, 100))
//                 if(toDrop = 0)
//                 then ()
//                 else printfn "XXX Dropping %i msgs from %s.input" toDrop (Participant.name p)
//                      let mutable c = toDrop //max 3 messages
//                      while (c > 0)
//                        do let () = q.Dequeue () |> ignore //drop
//                           c <- c - 1
            

        unfinished <- unfinishedParticipants participants
        let (canHandle,canSend) = unfinished
        c <- not (Array.isEmpty canHandle) || not (Array.isEmpty canSend)
  
  let drainQueue (buf:Queue<'a>) = 
    let mutable l = []
    while buf.Count <> 0
      do l <- buf.Dequeue () :: l
    l

