namespace Paxos.BasicPaxos.Test

open Paxos.BasicPaxos
open System.Collections.Generic


type Msg =
  | AMsg of AMsg
  | PMsg of PMsg
  | LMsg of LMsg
  | CMsg of CMsg

module Participant =
  type Acceptor = 
    { 
      Name : string
      Output : Queue<Destination * Msg>
      Input : Queue<string * Msg>
      mutable AState : AState 
      mutable CrashedFor : int
    }
  type Proposer = 
    { 
      Name : string
      Output : Queue<Destination * Msg>
      Input : Queue<string * Msg>
      mutable PState : PState
      mutable CrashedFor : int
    }
  type Learner = 
    { 
      Name : string
      Output : Queue<Destination * Msg>
      Input : Queue<string * Msg>
      mutable LState : LState
      mutable CrashedFor : int
    }
  type Client = 
    { 
      Name : string
      Output : Queue<Destination * Msg>
      Input : Queue<string * Msg>
    }

  
  type Type = 
  | AcceptorT
  | ProposerT
  | LearnerT
  | ClientT

  type Participant = 
    | Acceptor of Acceptor
    | Proposer of Proposer
    | Learner of Learner
    | Client of Client

  let freshAState = AReady (0,Map.empty)
  let freshPState = PReady 0
  let freshLState = LReady Map.empty

  let proposer name = 
    Proposer { 
      Name = name
      Output = Queue<Destination*Msg>()
      Input = Queue<string * Msg>()
      CrashedFor = 0
      PState = freshPState
    }
  let acceptor name = 
    Acceptor { 
      Name = name; 
      Output = Queue<Destination*Msg>(); 
      Input = Queue<string * Msg>(); 
      CrashedFor = 0;
      AState = freshAState;
    }
  let learner name = 
    Learner {
      Name = name
      Output = Queue<Destination*Msg>()
      Input = Queue<string * Msg>()
      CrashedFor = 0 
      LState = freshLState
    }
  let client name = 
    Client { 
      Name = name
      Output = Queue<Destination*Msg>()
      Input = Queue<string * Msg>()
    };

  let tryAcceptor a = match a with | (Acceptor x) -> Some x | _ -> None
  let tryProposer a = match a with | (Proposer x) -> Some x | _ -> None
  let tryLearner a = match a with | (Learner x) -> Some x | _ -> None
  let tryClient a = match a with | (Client x) -> Some x | _ -> None
  
  let isAcceptor = tryAcceptor >> Option.isSome
  let isProposer = tryProposer >> Option.isSome
  let isLearner = tryLearner >> Option.isSome
  let isClient = tryClient >> Option.isSome
  let isParticipantType pt p =
    match (pt,p) with 
    | (AcceptorT, Acceptor _) -> true
    | (ProposerT, Proposer _) -> true
    | (LearnerT, Learner _) -> true
    | (ClientT, Client _) -> true
    | (_,_) -> false

  let output p = 
    match p with 
    | Acceptor x -> x.Output
    | Proposer x -> x.Output
    | Learner x -> x.Output
    | Client x -> x.Output

  let input p = 
    match p with 
    | Acceptor x -> x.Input
    | Proposer x -> x.Input
    | Learner x -> x.Input
    | Client x -> x.Input

  let name p =
    match p with 
    | Acceptor x -> x.Name
    | Proposer x -> x.Name
    | Learner x -> x.Name
    | Client x -> x.Name

  let crashFor p rounds = 
    match p with 
    | Acceptor x -> x.CrashedFor <- rounds
    | Proposer x -> x.CrashedFor <- rounds
    | Learner x -> x.CrashedFor <- rounds
    | Client x -> ()

  let isCrashed p = 
    match p with 
    | Acceptor x -> x.CrashedFor > 0
    | Proposer x -> x.CrashedFor > 0
    | Learner x -> x.CrashedFor > 0
    | Client x -> false
  
  let decCrashedFor p = 
    match p with 
    | Acceptor x -> x.CrashedFor <- x.CrashedFor - 1
    | Proposer x -> x.CrashedFor <- x.CrashedFor - 1
    | Learner x -> x.CrashedFor <- x.CrashedFor - 1
    | Client x -> ()
  
  let find d ps = ps |> Seq.filter (fun a -> name a = d) |> Seq.head
  let findClient d ps = 
    ps 
    |> Seq.filter (fun p -> name p = d) 
    |> Seq.map (fun p -> match p with Client c -> c | _ -> failwith "not a client")
    |> Seq.exactlyOne
  
  let allA ps =
    ps
    |> Seq.filter isAcceptor
    |> Seq.map (fun p -> match p with Acceptor a -> a | _ -> failwith "not an acceptor")

  let sendTo m p =
    if (isCrashed p) 
    then ()
    else (input p).Enqueue m

  let sendToDest m d ps =
    let p = find d ps
    sendTo m p

  let broadcast ps m =
    ps 
    |> Seq.filter (fun p -> isAcceptor p || isProposer p || isLearner p)
    |> Seq.iter (sendTo m)

  let broadcastA ps m =
    ps 
    |> Seq.filter isAcceptor 
    |> Seq.iter (sendTo m)



