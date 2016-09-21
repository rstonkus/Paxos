namespace Paxos.BasicPaxos.Test

open Paxos.BasicPaxos
open System.Collections.Generic

module Participant =
  type Acceptor = 
    { Name : string
      mutable AState : AState 
      Output : Queue<Destination * Msg>
      Input : Queue<string * Msg>
      mutable Store : Map<Key,VersionedValue>
    }
  type Proposer = 
    { Name : string
      mutable PState : PState 
      Output : Queue<Destination * Msg>
      Input : Queue<string * Msg>
    }
  type Learner = 
    { Name : string
      mutable LState : unit 
      Output : Queue<Destination * Msg>
      Input : Queue<string * Msg>
    }
  type Client = 
    { Name : string
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

  let acceptor a = match a with | (Acceptor x) -> Some x | _ -> None
  let proposer a = match a with | (Proposer x) -> Some x | _ -> None
  let learner a = match a with | (Learner x) -> Some x | _ -> None
  let client a = match a with | (Client x) -> Some x | _ -> None
  
  let isAcceptor = acceptor >> Option.isSome
  let isProposer = proposer >> Option.isSome
  let isLearner = learner >> Option.isSome
  let isClient = client >> Option.isSome

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
    
  let findName n xs =
    match Seq.tryFind (fun x -> name x = n) xs with
    | None -> failwithf "no such destination %A" n
    | Some s -> s

  let allA ps = Seq.choose acceptor ps
  let allP ps = Seq.choose proposer ps
  let allL ps = Seq.choose learner ps
  let allC ps = Seq.choose client ps

  let findA d ps = allA ps |> Seq.filter (fun a -> a.Name = d) |> Seq.head
  let findP d ps = allP ps |> Seq.filter (fun a -> a.Name = d) |> Seq.head
  let findL d ps = allL ps |> Seq.filter (fun a -> a.Name = d) |> Seq.head
  let findC d ps = allC ps |> Seq.filter (fun a -> a.Name = d) |> Seq.head
    

  let iterA d ps f = findA d ps |> f
  let iterP d ps f = findP d ps |> f
  let iterL d ps f = findL d ps |> f
  let iterC d ps f = findC d ps |> f

  let iterAllA ps f = allA ps |> Seq.iter f
  let iterAllP ps f = allP ps |> Seq.iter f
  let iterAllL ps f = allL ps |> Seq.iter f