﻿namespace Paxos.BasicPaxos.Test

open Paxos.BasicPaxos
open System.Collections.Generic


type Msg =
  | AMsg of AMsg
  | PMsg of PMsg
  | LMsg of LMsg
  | EMsg of EMsg

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
      mutable CState : CState
      //mutable CrashedFor : int
    }
  type Learner = 
    { 
      Name : string
      Output : Queue<Destination * Msg>
      Input : Queue<string * Msg>
      mutable LState : LState
      //mutable CrashedFor : int
    }
  type External =
   {
      Name : string
      Output : Queue<Destination * Msg>
      Input : Queue<string * Msg>
   }
  

  
  type Type = 
  | AcceptorT
  | ProposerT
  | LearnerT
  | ExternalT

  type Participant = 
    | Acceptor of Acceptor
    | Proposer of Proposer
    | Learner of Learner
    | External of External

  let freshAState = AReady (0,Map.empty)
  let freshPState = PReady 0
  let freshLState = LReady Map.empty
  let freshCState = CInitial

  let proposer name = 
    Proposer { 
      Name = name
      Output = Queue<Destination*Msg>()
      Input = Queue<string * Msg>()
//      CrashedFor = 0
      PState = freshPState
      CState = freshCState
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
//      CrashedFor = 0 
      LState = freshLState
    }
  let external name = 
    External { 
      Name = name
      Output = Queue<Destination*Msg>()
      Input = Queue<string * Msg>()
    };

  let tryAcceptor a = match a with | (Acceptor x) -> Some x | _ -> None
  let tryProposer a = match a with | (Proposer x) -> Some x | _ -> None
  let tryLearner a = match a with | (Learner x) -> Some x | _ -> None
  let tryExternal a = match a with | (External x) -> Some x | _ -> None
  
  let isAcceptor = tryAcceptor >> Option.isSome
  let isProposer = tryProposer >> Option.isSome
  let isLearner = tryLearner >> Option.isSome
//  let isClient = tryClient >> Option.isSome
  let isParticipantType pt p =
    match (pt,p) with 
    | (AcceptorT, Acceptor _) -> true
    | (ProposerT, Proposer _) -> true
    | (LearnerT, Learner _) -> true
    | (ExternalT, External _) -> true
    | (_,_) -> false

  let output p = 
    match p with 
    | Acceptor x -> x.Output
    | Proposer x -> x.Output
    | Learner x -> x.Output
    | External x -> x.Output

  let input p = 
    match p with 
    | Acceptor x -> x.Input
    | Proposer x -> x.Input
    | Learner x -> x.Input
    | External x -> x.Input

  let name p =
    match p with 
    | Acceptor x -> x.Name
    | Proposer x -> x.Name
    | Learner x -> x.Name
    | External x -> x.Name

  let crashFor p rounds = 
    match p with 
    | Acceptor x -> x.CrashedFor <- rounds
    | Proposer x -> ()
    | Learner x -> ()
    | External x -> ()

  let isCrashed p = 
    match p with 
    | Acceptor x -> x.CrashedFor > 0
    | Proposer x -> false
    | Learner x -> false
    | External x -> false
  
  let decCrashedFor p = 
    match p with 
    | Acceptor x -> if x.CrashedFor > 0 then x.CrashedFor <- x.CrashedFor - 1
    | Proposer x -> ()
    | Learner x -> ()
    | External x -> ()
  
  let find d ps = ps |> Seq.filter (fun a -> name a = d) |> Seq.head
  let findExternal d ps = 
    ps 
    |> Seq.filter (fun p -> name p = d) 
    |> Seq.map (fun p -> match p with External c -> c | _ -> failwith "not an external")
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



