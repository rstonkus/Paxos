namespace Paxos

open System
open System.Collections.Generic

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
// replication to acceptor when starting up
  
  type Time = int
  type N = int
  type Sender = string
  type Key = string
  type Value = string
  type ValueLastTouched = Value * (Sender * Guid) list //"sender*guid list" makes it possible to check for execution of a concrete operation
  
  type Destination =
    | Proposer of Sender
    | Broadcast
    | BroadcastAcceptors
    | External of Sender
    | Acceptor of Sender
  

  type VersionedValueLastTouched = N * ValueLastTouched 
  type AcceptorStore = Map<Key,VersionedValueLastTouched>
  
  //TODO we must remember the old value when building up quorum for the new
  type LearnerStore = Map<Key, N * Sender list> //Sender is necessary in order to make sure that it is not a duplicate

  //Request
  type Operation = string * (ValueLastTouched option -> ValueLastTouched)
  type RequestSession = Guid * Key
  type Request = Sender * RequestSession * Operation
  type ExternalRequest = RequestSession * Operation
 

  //learner msgs
  type LMsg = 
    | MResponse of Guid * ValueLastTouched

  //proposer msgs
  type PMsg =
    | MPrepare of N * Key
    | MAccept of N * Sender * RequestSession * ValueLastTouched //new value

  //acceptors msgs
  type AMsg = 
    //Sent in response to a Prepare to only the requesting proposer
    | MPromise of N * VersionedValueLastTouched option //session n, versioned value stored on that particular node
    //Broadcast to all proposers and learners in case of success
    | MAccepted of N * Sender * RequestSession * ValueLastTouched //this session, (value session (=this session), value)
    //Sent in response to a Prepare to only the requesting proposer
    //in case the acceptor is ahead of the proposed session n.
    | MNackPrepare of N * N//session n, head n at acceptor (It will not accept smaller ns)
    | MSyncRequest
    | MSyncResponse of N * AcceptorStore

  type EMsg =
    | MExternalRequest of Sender * ExternalRequest


  //For now the session state is shared by all keys. This is suboptimal if one wants more than one value in the system.
  //It could be extended to support several concurrent keys (and adjusting the code accordingly): 
  //Essentially: type PState = Map<Key,PStateOld>
  //                  AState = Map<Key,AStateOld>

  type CState =
    | CReady
    | CActiveRequest of Queue<ExternalRequest> * Sender * Time * ExternalRequest

  type PState =
    //No session in progress. N = Last tried session that we know
    | PReady of N 
    //State of collecting promises in order to reach quorum
    | PPrepareSent of N * Request * (VersionedValueLastTouched option) list // I sent proposal with n,v, received promise list so far
    //State of collecting accepted messages 
    //this is a potential stuck state, since we are not guaranteed to receive MAccepted from quorum 
    // => We need to implement some sort of detection in the client.
    //    in case another proposer initiates a session, we will however have progress.
    | PAcceptSent of N * Request * VersionedValueLastTouched list// I sent accept with n,v, received accept list so far

  type AState =
    | ASync of N list * AcceptorStore * (N * Sender) list //(incremental updates handled(i.e. accepted messages) , merged store, sources from which we have received store)
    | AReady of N * AcceptorStore//previous highest proposed

  type LState =
    | LReady of LearnerStore




  //helpers
  let isQuorum quorumSize xs = quorumSize <= Seq.length xs
  
//  let hasTimedOut time =
//    let now = DateTime.Now
//    now - time > (TimeSpan.FromSeconds 2.0)
    

  let latestValue (vs:(VersionedValueLastTouched option) list) : ValueLastTouched option = 
    (List.maxBy (fun v -> match v with None -> -1 | Some (n,_) -> n) vs) |> Option.map snd

  //msg handlers
  let proposerReceiveFromAcceptor (quorumSize:int) (now:Time) (cs:CState) (ps:PState) (m:AMsg) : (CState * PState * (Destination * PMsg) option) =
    let ignore = (cs,ps,None)
    let retry (r:Request) n =
      let (requester,session,f) = r
      let (g,k) = session
      in (cs, PPrepareSent (n,r,[]), Some (BroadcastAcceptors, MPrepare (n,k)))
    let next n =
      match cs with
      | CReady -> failwith "bug"
      | CActiveRequest (q,requester,time,er) ->
        if (q.Count = 0)
        then (CReady, PReady n, None)
        else let er = q.Dequeue ()
             let (session,f) = er
             let (g,k) = session
             let r = (requester,session,f)
             in (CActiveRequest (q,requester,now,er), PPrepareSent (n,r,[]), Some (BroadcastAcceptors, MPrepare (n,k)))
    match ps with
    | PReady n -> //This session is initiated by another proposer
      match m with
      | MAccepted (n',_,_,_) -> 
        if (n' > n)
        then (cs, PReady n', None) //update session
        else ignore
      | MNackPrepare (_,n') ->
        if (n' > n)
        then (cs, PReady n',None)
        else ignore
      | MPromise _ -> ignore
    | PPrepareSent (n, r, promises) ->
      match m with
      | MPromise (n', v') -> 
        if (n = n') //it is a promise in this session initiated by me
        then let promises' = v' :: promises
             let (requester,session,op) = r
             let (fname,f) = op
             if (isQuorum quorumSize promises')
             then let maxV = promises' |> latestValue
                  let v' = f maxV
                  //in (PReady n, Some (BroadcastAcceptors, MAccept(n,cs,k,v')))
                  in (cs, PAcceptSent(n, r, []), Some (BroadcastAcceptors, MAccept (n,requester,session,v'))) //send accept
             else (cs, PPrepareSent(n, r, promises'), None) //wait for more promises
        else ignore //it can only be an old one, since it is a response to a request sent by us. Ignore
      | MAccepted _ -> ignore //Old => Ignore
      | MNackPrepare (n',newN) -> 
        if (n = n')
        then retry r (newN+1)
        else ignore
      | MSyncRequest _ -> ignore
    
    | PAcceptSent (n, cr, accepts) ->
      match m with
      | MPromise _ -> ignore //either from another session, or a promise that was not needed for this one. Ignore
      | MAccepted (n', requester, session, v) ->
        if (n = n') //it is an accept in this session initiated by me
        then let accepts' = (n,v) :: accepts
             if (isQuorum quorumSize accepts')
             //session done, assume learner will answer client.
             then next (n + 1)
             else (cs, PAcceptSent (n,cr,accepts'), None) //collect accept
        else if (n < n')
             then retry cr (n' + 1)
             else ignore //it was an old one. Ignore
      | MNackPrepare _ -> ignore //it could have only come from an old session


  //note that the client is modelled as running locally with the proposer 
  let clientCheckActiveRequest (cs:CState) (ps:PState) (now:Time) (hastimedout:Time -> bool) : (CState * PState * (Destination * PMsg) option) =
    let ignore = (cs,ps,None)
    let restart q requester n (er:ExternalRequest) = 
      let n' = n+1
      let (session,f) = er
      let (g,k) = session
      let r = (requester,session,f)
      in (CActiveRequest (q,requester,now,er), PPrepareSent (n',r,[]), Some (BroadcastAcceptors, MPrepare (n',k)))
    match cs with
    | CReady -> ignore
    | CActiveRequest (q,requester,time,er) ->
      if hastimedout time
      then match ps with
           | PReady _ -> failwith "Bug. A ready state should always start the next round immediately in case any is ready"
           | PPrepareSent (n,_,_) -> restart q requester n er
           | PAcceptSent (n,_,_) -> restart q requester n er
      else ignore 


  let proposerReceiveRequest (cs:CState) (ps:PState) (now:Time) (MExternalRequest(requester,er):EMsg) : (CState * PState * (Destination * PMsg) option) =
    let handleRequest q n (er:ExternalRequest) = 
      let n' = n+1
      let (session,f) = er
      let (g,k) = session
      let r = (requester,session,f)
      in (CActiveRequest (q,requester, now,er), PPrepareSent (n',r,[]), Some (BroadcastAcceptors, MPrepare (n',k)))
    match cs with
    | CReady ->
      match ps with
      | PReady n -> 
        let q = Queue<ExternalRequest>()
        in handleRequest q n er
      | _ -> failwith "Bug. When client is in CInitial, proposer cannot have an active session."
    | CActiveRequest (q,_,_,_) ->
      let () = q.Enqueue er
      in (cs,ps,None)


  let acceptorReceiveFromProposer (s:AState) (m:PMsg) (sender:Sender) : (AState * (Destination * AMsg) option) =
    let ignore = (s, None)
    match s with
    | AReady (n,store) -> 
      match m with
      | MPrepare (n',k) -> 
        if (n < n')
        then (AReady (n', store), Some (Proposer sender, MPromise (n', Map.tryFind k store)))
        else (AReady (n, store), Some (Proposer sender, MNackPrepare (n',n)))
      | MAccept (n', requester, session, v') ->
        let (_, k') = session
        if (n <= n') 
        then let store' = Map.add k' (n',v') store
             in (AReady (n', store'), Some (Broadcast, MAccepted (n', requester, session, v')))
        else (AReady (n, store), Some (Proposer sender, MNackPrepare (n',n)))
    | ASync _ -> ignore

  let acceptorRestart () : (AState * (Destination * AMsg)) =
    let s = ASync ([], Map.empty, [])
    let d = (BroadcastAcceptors, MSyncRequest)
    in (s, d)

  let acceptorReceiveFromAcceptor (quorumSize:int) (s:AState) (m:AMsg) (sender:Sender) : (AState * (Destination * AMsg) option) =
    let ignore = (s, None)
    let addIfNewer k (n,v) (m:AcceptorStore) =
      match Map.tryFind k m with
        | None -> Map.add k (n,v) m
        | Some (n',v') -> if (n > n')
                          then Map.add k (n,v) m
                          else m
    let mergeStores mstore store = 
      Map.fold (fun m k v -> addIfNewer k v m) mstore store
    match s with
    | AReady (n,store) -> 
      match m with 
      | MAccepted (n',requester,(g,k),v) ->
        let store' = addIfNewer k (n',v) store
        if n' > n
        then (AReady (n',store'), None)
        else (AReady (n,store'), None) //store' might be unmodified
      | MSyncRequest ->
        (s, Some (Acceptor sender, MSyncResponse (n,store)))
      | _ -> ignore

    | ASync (ns,store,votes) -> 
      match m with
      | MAccepted (n',requester,(g,k),v) ->
        match ns with
        | [] -> 
          let store' = addIfNewer k (n',v) store
          in (ASync ([n'],store',votes), None)
        | n :: ns -> 
          let nMax = Seq.max (n::ns)
          if n' > nMax
          then let store' = addIfNewer k (n',v) store
               in (ASync (n'::ns,store',votes), None)
          else ignore
        
      | MSyncResponse (mn,mstore) ->
        if (Seq.exists (snd >> ((=) sender)) votes)
        then ignore
        else let store' = mergeStores mstore store //take newest from each
             let votes' = (mn,sender) :: votes
             if (isQuorum quorumSize votes')
             then let maxFromStore = Map.fold (fun m k (n,v) -> max m n) 0 store'
                  match ns with
                  | [] -> (AReady (maxFromStore,store'),None)
                  | _ -> //let maxN = Seq.max ns //here we might need to check for sequence of the collected list mentioned in above TODO
                         //let newN = max maxN maxFromStore
                         (AReady (maxFromStore,store'),None)
             else (ASync (ns,store',votes'),None)
        
      //dont respond to session messages while synching
      | MPromise (n,vvo) -> ignore
      | MNackPrepare (n,n') -> ignore
      | MSyncRequest -> ignore
      

  let learnerReceiveFromAcceptor (LReady store : LState) (m:AMsg) (sender:Sender) (quorumSize:int): (LState * (Destination * LMsg) option) = 
    let ignore = (LReady store, None)
    let firstVote k n = 
      let store' = Map.add k (n, [sender]) store //update store to reflect one vote
      in (LReady store', None) //never quorum with just one
    match m with
    | MAccepted (n, requester, session, v) -> 
      let (guid,k) = session
      let storeV = Map.tryFind k store
      match storeV with
      | None -> firstVote k n
      | Some (nStore,votes) -> 
        if (n = nStore) //we are interested in the vote
        then let votes' = sender :: votes //collect vote first
             let store' = Map.add k (nStore,votes') store //update store
             in if ((not (isQuorum quorumSize votes)) && (isQuorum quorumSize votes')) //if we got to quorum this time
                then (LReady store', Some (External requester, MResponse (guid,v)))
                else (LReady store', None)
        else if (n > nStore)
             then firstVote k n //new vote round
             else ignore //old vote round
    //these are not meant to be received by learner
    | MPromise _ -> ignore
    | MNackPrepare _ -> ignore
    | MSyncRequest _ -> ignore
    
