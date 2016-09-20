namespace Paxos

open System

module BasicPaxos =

//https://en.wikipedia.org/wiki/Paxos_(computer_science)
//
//This protocol is the most basic of the Paxos family. Each instance of the Basic Paxos protocol decides on a single output value. 
//The protocol proceeds over several rounds. A successful round has two phases. A Proposer should not initiate Paxos if it cannot 
//communicate with at least a Quorum of Acceptors:
//
//Phase 1a: Prepare[edit]
//A Proposer (the leader) creates a proposal identified with a number N. This number must be greater than any previous proposal 
//number used by this Proposer. Then, it sends a Prepare message containing this proposal to a Quorum of Acceptors. The Proposer 
//decides who is in the Quorum.

//Phase 1b: Promise[edit]
//If the proposal's number N is higher than any previous proposal number received from any Proposer by the Acceptor, then the 
//Acceptor must return a promise to ignore all future proposals having a number less than N. If the Acceptor accepted a proposal 
//at some point in the past, it must include the previous proposal number and previous value in its response to the Proposer.
//Otherwise, the Acceptor can ignore the received proposal. It does not have to answer in this case for Paxos to work. However, 
//for the sake of optimization, sending a denial (Nack) response would tell the Proposer that it can stop its attempt to create 
//consensus with proposal N.

//Phase 2a: Accept Request[edit]
//If a Proposer receives enough promises from a Quorum of Acceptors, it needs to set a value to its proposal. If any Acceptors had 
//previously accepted any proposal, then they'll have sent their values to the Proposer, who now must set the value of its 
//proposal to the value associated with the highest proposal number reported by the Acceptors. If none of the Acceptors had 
//accepted a proposal up to this point, then the Proposer may choose any value for its proposal.[17]
//The Proposer sends an Accept Request message to a Quorum of Acceptors with the chosen value for its proposal.

//Phase 2b: Accepted[edit]
//If an Acceptor receives an Accept Request message for a proposal N, it must accept it if and only if it has not already promised 
//to any prepare proposals having an identifier greater than N. In this case, it should register the corresponding value v and send 
//an Accepted message to the Proposer and every Learner. Else, it can ignore the Accept Request.
//Note that an Acceptor can accept multiple proposals. These proposals may even have different values in the presence of certain 
//failures. However, the Paxos protocol will guarantee that the Acceptors will ultimately agree on a single value.
//Rounds fail when multiple Proposers send conflicting Prepare messages, or when the Proposer does not receive a Quorum of responses
// (Promise or Accepted). In these cases, another round must be started with a higher proposal number.
//Notice that when Acceptors accept a request, they also acknowledge the leadership of the Proposer. Hence, Paxos can be used to
// select a leader in a cluster of nodes.
//Here is a graphic representation of the Basic Paxos protocol. Note that the values returned in the Promise message are null the 
//first time a proposal is made, since no Acceptor has accepted a value before in this round.
//
//  
//Basic Paxos:
//
//   Client   Proposer      Acceptor     Learner
//     |         |          |  |  |       |  |
//     X-------->|          |  |  |       |  |  Request
//     |         X--------->|->|->|       |  |  Prepare(1)
//     |         |<---------X--X--X       |  |  Promise(1,{Va,Vb,Vc})
//     |         X--------->|->|->|       |  |  Accept!(1,Vn)
//     |         |<---------X--X--X------>|->|  Accepted(1,Vn)
//     |<---------------------------------X--X  Response
//     |         |          |  |  |       |  |
//
//Multi Paxos:
//
//   Client   Proposer      Acceptor     Learner
//   (...)
//     |         |          |  |  |       |  |  --- Following Requests ---
//     X-------->|          |  |  |       |  |  Request
//     |         X--------->|->|->|       |  |  Accept!(N,I+1,W)
//     |         |<---------X--X--X------>|->|  Accepted(N,I+1,W)
//     |<---------------------------------X--X  Response
//     |         |          |  |  |       |  |

  
  type N = int
  type Value = string
  type Key = string
  //For now the VersionState is shared by all keys. This is suboptimal if one wants more than one value in the system.
  //It could be extended to support several concurrent keys (and adjusting the code accordingly): 
  //type VersionState = Map<ValueId,N>
  type VersionState = N
  type Sender = string

  type Destination =
    | Proposer of Sender
    | BroadcastProposersLearners
    | BroadcastAcceptors
    | Client of Sender
  
  type VersionedValue = N * Value
  type ClientRequest = (Guid * Sender) * Key * (VersionedValue option -> Value)
  type Store = Map<Key,VersionedValue>

  //client msgs
  type CMsg = 
    | MClientRequest of ClientRequest
//  | MForceReset

  type LMsg = 
    | MResponse of Value

  //proposer msgs
  type PMsg =
    | MPrepare of N * Key
    | MAccept of N * Key * Value //new value
    | MClientNack of Guid * string //guid, reason

  //Msgs sent from acceptors
  type AMsg = 
    //Sent in response to a Prepare to only the requesting proposer
    | MPromise of N * VersionedValue option //session n, versioned value stored on that particular node
    //Sent in response to a Prepare to only the requesting proposer
    //in case the acceptor is ahead of the proposed session n.
    | MNackPrepare of N //head n at acceptor (It will not accept smaller ns)
    //Broadcast to all proposers and learners in case of success
    | MAccepted of N * Key * Value //this session, (value session (=this session), value)

  type Msg =
    | AMsg of AMsg
    | PMsg of PMsg
    | LMsg of LMsg
    | CMsg of CMsg

  type PState =
    //No session in progress. N = Last tried session that we know
    | PWaiting of VersionState 
    //State of collecting promises in order to reach quorum
    | PPrepareSent of VersionState * ClientRequest * (VersionedValue option) list // I sent proposal with n,v, received promise list so far
    //State of collecting accepted messages 
    | PAcceptSent of VersionState * ClientRequest * VersionedValue list// I sent accept with n,v, received accept list so far

  type AState =
    | AReady of VersionState //previous highest proposed

  let isQuorum quorumSize xs = quorumSize > List.length xs

  let latestValue (vs:(VersionedValue option) list) : VersionedValue option = 
    List.maxBy (fun v -> match v with None -> -1 | Some (n,_) -> n) vs

  let proposerReceiveFromAcceptor (quorumSize:int) (s:PState) (m:AMsg) : (PState * (Destination * PMsg) Option) =
    let ignore = (s,None)
    let nackClient s cr reason = 
      let ((guid,sender),_,_) = cr
      in (s, Some (Client sender, MClientNack (guid, reason)))
    match s with
    | PWaiting n -> //This session is initiated by another proposer
      match m with
      | MAccepted (n',_,_) -> 
        if (n' > n)
        then (PWaiting n', None) //update session
        else ignore
      | _ -> ignore //ignore all other msgs
    | PPrepareSent (n, cr, promises) ->
      match m with
      | MPromise (n', v') -> 
        if (n = n') //it is a promise in this session initiated by me
        then let promises' = v' :: promises
             let (_,valId,f) = cr
             if (isQuorum quorumSize promises')
             then let maxV = promises' |> latestValue
                  let v' = f maxV
                  in (PAcceptSent(n, cr, []), Some (BroadcastAcceptors, MAccept(n,valId,v'))) //send accept
             else (PPrepareSent(n, cr, promises'), None) //wait for more promises
        else if (n < n')
             then nackClient (PWaiting n') cr "proposer behind" //if it is newer we cancel session and up the n to the newest
             else (PPrepareSent (n, cr, promises), None) //it was an old one. Ignore
      | MAccepted _ -> ignore //Old => Ignore
      | MNackPrepare n' -> if (n < n')
                           then nackClient (PWaiting n') cr "proposer behind" //The nack proposes a newer n. It does not matter if it was a response to this session directly. Abort.
                           else ignore //Old => Ignore. 
    | PAcceptSent (n, cr, accepts) ->
      match m with
      | MPromise _ -> ignore //either from another session, or a promise that was not needed for this one. Ignore
      | MAccepted (n', _, v) ->
        if (n = n') //it is an accept in this session initiated by me
        then let accepts' = (n,v) :: accepts
             if (isQuorum quorumSize accepts')
             then (PWaiting n, None) //session done, assume learner will answer client.
             else (PAcceptSent (n, cr, accepts), None)
        else if (n < n')
             then nackClient (PWaiting n') cr "proposer behind" //if it is newer we cancel session and up the n to the newest
             else (PAcceptSent (n, cr, accepts), None) //it was an old one. Ignore
      | MNackPrepare n' -> 
        if (n < n')
        then nackClient (PWaiting n') cr "proposer behind"
        else ignore

  let proposerReceiveFromClient (s:PState) (m:CMsg) : (PState * (Destination * PMsg) Option) =
    let nackClient s cr reason = 
      let ((guid,sender),_,_) = cr
      in (s, Some (Client sender, MClientNack (guid, reason)))
    match s with 
    | PWaiting n ->
      match m with
      | MClientRequest cr -> 
        let n' = n+1
        let (_,valId,_) = cr
        in (PPrepareSent (n',cr,[]), Some (BroadcastAcceptors, MPrepare (n',valId)))
//      | MForceReset -> (s, None) //already waiting
    | PPrepareSent (n,cr,_) -> 
      match m with
      | MClientRequest cr' -> nackClient s cr' "busy" //This is not meant to happen. The client is local, so requests should be buffered and only sent, when proposer is PWaiting.
//      | MForceReset -> nackClient (PWaiting n) cr "forced reset"
    | PAcceptSent (n,cr,_) -> 
      match m with
      | MClientRequest cr' -> nackClient s cr' "busy"
//      | MForceReset -> nackClient (PWaiting n) cr "forced reset"


  let acceptorReceiveFromProposer (AReady n:AState) (m:PMsg) (sender:Sender) (store:Store) : (AState * Store * (Destination * AMsg) Option) =
    let ignore = (AReady n,store,None)
    match m with
    | MPrepare (n',valId) -> 
      if (n <= n')
      then (AReady n', store, Some (Proposer sender, MPromise (n', Map.tryFind valId store)))
      else (AReady n', store, Some (Proposer sender, MNackPrepare n))
    | MAccept (n',valId',v') ->
      if (n <= n') 
      then let store' = Map.add valId' (n',v') store
            in (AReady n', store', Some (BroadcastProposersLearners, MAccepted (n, valId', v')))
      else (AReady n, store, Some (Proposer sender, MNackPrepare n))
    | MClientNack _ -> ignore //not relevant for acceptor

  
