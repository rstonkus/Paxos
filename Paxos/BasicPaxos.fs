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

//TODO 
// retry in proposer
// handle at least once delivery (or argue that we dont need it)
// timeout
  
  type N = int
  type Value = string
  type Key = string

  type Sender = string

  type Destination =
    | Proposer of Sender
    | Broadcast
    | BroadcastAcceptors
    | Client of Sender
  
  type VersionedValue = N * Value
  type ClientSession = Guid * Sender
  type ClientRequest = ClientSession * Key * (VersionedValue option -> Value)
  type AcceptorStore = Map<Key,VersionedValue>
  
  type LearnerStore = Map<Key, N * Sender list> //Sender is necessary in order to make sure that it is not a duplicate
  

  //Messages are split into different types depending on who sends them
  //client msgs
  type CMsg = 
    | MClientRequest of ClientRequest

  //learner msgs
  type LMsg = 
    | MResponse of Guid * Value

  //proposer msgs
  type PMsg =
    | MPrepare of N * Key
    | MAccept of N * ClientSession * Key * Value //new value
    | MClientNack of Guid * string //guid, reason

  //acceptors msgs
  type AMsg = 
    //Sent in response to a Prepare to only the requesting proposer
    | MPromise of N * VersionedValue option //session n, versioned value stored on that particular node
    //Broadcast to all proposers and learners in case of success
    | MAccepted of N * ClientSession * Key * Value //this session, (value session (=this session), value)
    //Sent in response to a Prepare to only the requesting proposer
    //in case the acceptor is ahead of the proposed session n.
    | MNackPrepare of N * N//session n, head n at acceptor (It will not accept smaller ns)



  //For now the session state is shared by all keys. This is suboptimal if one wants more than one value in the system.
  //It could be extended to support several concurrent keys (and adjusting the code accordingly): 
  //Essentially: type PState = Map<Key,PStateOld>
  //                  AState = Map<Key,AStateOld>

  type PState =
    //No session in progress. N = Last tried session that we know
    | PReady of N 
    //State of collecting promises in order to reach quorum
    | PPrepareSent of N * ClientRequest * (VersionedValue option) list // I sent proposal with n,v, received promise list so far
    //State of collecting accepted messages 
    | PAcceptSent of N * ClientRequest * VersionedValue list// I sent accept with n,v, received accept list so far

  type AState =
    | AReady of N * AcceptorStore//previous highest proposed

  type LState =
    | LReady of LearnerStore


  //helpers
  let isQuorum quorumSize xs = quorumSize <= List.length xs

  let latestValue (vs:(VersionedValue option) list) : VersionedValue option = 
    List.maxBy (fun v -> match v with None -> -1 | Some (n,_) -> n) vs

  //msg handlers
  let proposerReceiveFromAcceptor (quorumSize:int) (s:PState) (m:AMsg) : (PState * (Destination * PMsg) option) =
    let ignore = (s,None)
    //for now, we will push the action of retrying to the client.
    let nackClient s cr reason = 
      let ((guid,sender),_,_) = cr
      in (s, Some (Client sender, MClientNack (guid, reason)))
    let nackPrepare cr n n' newN =
      if (n = n')
      then if(newN > n)
            then nackClient (PReady newN) cr "proposer behind" //The nack proposes a newer n. It does not matter if it was a response to this session directly. Abort.
            else nackClient s cr "proposer behind"
      else ignore //Different session (old, since it is a response) => Ignore. 
    match s with
    | PReady n -> //This session is initiated by another proposer
      match m with
      | MAccepted (n',_,_,_) -> 
        if (n' > n)
        then (PReady n', None) //update session
        else ignore
      | _ -> ignore //ignore all other msgs
    | PPrepareSent (n, cr, promises) ->
      match m with
      | MPromise (n', v') -> 
        if (n = n') //it is a promise in this session initiated by me
        then let promises' = v' :: promises
             let (cs,k,f) = cr
             if (isQuorum quorumSize promises')
             then let maxV = promises' |> latestValue
                  let v' = f maxV
                  in (PAcceptSent(n, cr, []), Some (BroadcastAcceptors, MAccept(n,cs,k,v'))) //send accept
             else (PPrepareSent(n, cr, promises'), None) //wait for more promises
        else if (n < n')
             then nackClient (PReady n') cr "proposer behind" //if it is newer we cancel session and up the n to the newest
             else ignore //it was an old one. Ignore
      | MAccepted _ -> ignore //Old => Ignore
      | MNackPrepare (n',newN) -> nackPrepare cr n n' newN
    | PAcceptSent (n, cr, accepts) ->
      match m with
      | MPromise _ -> ignore //either from another session, or a promise that was not needed for this one. Ignore
      | MAccepted (n', _, _, v) ->
        if (n = n') //it is an accept in this session initiated by me
        then let accepts' = (n,v) :: accepts
             if (isQuorum quorumSize accepts')
             then (PReady n, None) //session done, assume learner will answer client.
             else (PAcceptSent (n,cr,accepts'), None) //collect accept
        else if (n < n')
             then nackClient (PReady n') cr "proposer behind" //if it is newer we cancel session and up the n to the newest
             else ignore //it was an old one. Ignore
      | MNackPrepare (n',newN) -> nackPrepare cr n n' newN


  //note that the client is modelled as running locally on the proposer 
  let proposerReceiveFromClient (s:PState) (MClientRequest cr:CMsg) : (PState * (Destination * PMsg) Option) =
    let nackClient s cr reason = 
      let ((guid,sender),_,_) = cr
      in (s, Some (Client sender, MClientNack (guid, reason)))
    match s with 
    | PReady n ->
      let n' = n+1
      let (_,k,_) = cr
      in (PPrepareSent (n',cr,[]), Some (BroadcastAcceptors, MPrepare (n',k)))
    //These are not meant to happen. Since the client is local, requests should be buffered and 
    //only sent, when proposer is PReady.
    | PPrepareSent _ -> nackClient s cr "busy"
    | PAcceptSent _ -> nackClient s cr "busy"
    

  let acceptorReceiveFromProposer (AReady (n,store):AState) (m:PMsg) (sender:Sender) : (AState * (Destination * AMsg) option) =
    let ignore = (AReady (n,store), None)
    match m with
    | MPrepare (n',k) -> 
      if (n < n')
      then (AReady (n', store), Some (Proposer sender, MPromise (n', Map.tryFind k store)))
      else (AReady (n', store), Some (Proposer sender, MNackPrepare (n',n)))
    | MAccept (n',cs, k',v') ->
      if (n <= n') 
      then let store' = Map.add k' (n',v') store
           in (AReady (n', store'), Some (Broadcast, MAccepted (n, cs, k', v')))
      else (AReady (n, store), Some (Proposer sender, MNackPrepare (n',n)))
    | MClientNack _ -> ignore //not relevant for acceptor

  let acceptorReceiveFromAcceptor (AReady (n,store):AState) (m:AMsg) (sender:Sender) : (AState * (Destination * AMsg) option) =
    let ignore = (AReady (n,store), None)
    ignore //TODO

  let learnerReceiveFromAcceptor (LReady store : LState) (m:AMsg) (sender:Sender) (quorumSize:int): (LState * (Destination * LMsg) option) = 
    let ignore = (LReady store, None)
    let firstVote k n = 
      let store' = Map.add k (n, [sender]) store //update store to reflect one vote
      in (LReady store', None) //never quorum with just one
    match m with
    | MAccepted (n, (guid,clientSender), k, v) -> 
      let storeV = Map.tryFind k store
      match storeV with
      | None -> firstVote k n
      | Some (nStore,votes) -> 
        if (n = nStore) //we are interested in the vote
        then let votes' = sender :: votes //collect vote first
             let store' = Map.add k (nStore,votes') store //update store
             in if ((not (isQuorum quorumSize votes)) && (isQuorum quorumSize votes')) //if we got to quorum this time
                then (LReady store', Some (Client clientSender, MResponse (guid,v)))
                else (LReady store', None)
        else if (n > nStore)
             then firstVote k n //new vote round
             else ignore //old vote round
    | MPromise _ -> ignore
    | MNackPrepare _ -> ignore
    
