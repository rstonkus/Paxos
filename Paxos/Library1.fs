namespace Paxos

module LeaderElection =
  
  //Of the following protocol we will ignore the values in the promise message,
  //since the purpose of the interaction is to find out who should be the leader.
  //
  //So, in this case, the client is not relevant. The session will be initiated by any who decides it is time
  //for a leader election.
  //
  //Unless the session resolves in quorum, no leader will be elected.
  //
  //Any member of the cluster will remain a (possibly dead) member until the cluster is reconfigured.
  //This could be done dynamically, and this process would follow the basic paxos protocol below.
  //
  //Basic Paxos:
  //
  //   Client   Proposer      Acceptor     Learner
  //    |         |          |  |  |       |  |
  //    X-------->|          |  |  |       |  |  Request
  //    |         X--------->|->|->|       |  |  Prepare(1)
  //    |         |<---------X--X--X       |  |  Promise(1,{Va,Vb,Vc})
  //    |         X--------->|->|->|       |  |  Accept!(1,Vn)
  //    |         |<---------X--X--X------>|->|  Accepted(1,Vn)
  //    |<---------------------------------X--X  Response
  //    |         |          |  |  |       |  |
  //
  //
  //


  type V = string
  type Sender = string
  type N = Sender * int //Initiator * session id

  //proposer msgs
  type PMsg = 
    | MPrepare of Sender * N
    | MAccept of Sender * N * V
  //accepter msgs
  type AMsg = 
    | MPromise of Sender * N * N * V //Msg sender, this session, (value session, value)
    | MAccepted of Sender * N * V //Msg sender, this session, value

  type Promise = N * V //value session, value
  type Accept = N * V //this session, value
  type PState =
    | PWaiting of N //previous proposal no
    | PPrepareSent of N * V * Promise list // I sent proposal with n,v, received promise list so far
    | PAcceptSent of N * V * Accept list// I sent accept with n,v, received accept list so far
    | PLeader of N

  type AState =
    | AWaiting of N //previous highest proposed
    | APromised of N * V //I promised other proposer
    | AAccepted of N * V //sending back concensus

  let inc ((sender, n):N) : N = (sender,n + 1)
  let eq ((sender1, n1):N) ((sender2, n2):N) = sender1 = sender2 && n1 = n2
  let isNewer ((_, n1):N) ((_, n2):N) = n2 > n1

  let acceptorReceive (s:AState) (m:PMsg) : (AState * AMsg Option) =
    match m with
    | MPrepare _ -> 
      match s with
      | AWaiting n -> (s,None) 
      | APromised (n,v) -> (s,None)
      | AAccepted (n,v) -> (s,None)
    | MAccept _ ->
      match s with
      | AWaiting n -> (s,None) 
      | APromised (n,v) -> (s,None)
      | AAccepted (n,v) -> (s,None)
        
  let isQuorum (ps:Promise list) = true //TODO
  //Disagreement will never happen in a friendly env.
  let isAgreement (v:V) (ps:Promise list) = true

  let isQuorum (ps:Accept list) = true //TODO
  //Disagreement will never happen in a friendly env.
  let isAgreement (v:V) (ps:Accept list) = true

  let proposerReceive (s:PState) (m:AMsg) : (PState * PMsg Option) =
    match s with
    | PWaiting n -> (s,None) //This session is initiated by another proposer. Maybe update n?
    | PPrepareSent (n, v, promises) ->
      match m with
      | MPromise (sender, np, vp) -> 
        if (eq n np) //it is a promise in this session initiated by me
        then let promises' = (sender, n, v) :: promises
             if ((isQuorum promises') && (isAgreement v promises')) 
             then (PAcceptSent(n, v, []), Some (MAccept(sender, n, v))) //send accept
             else (PPrepareSent(n, v, promises'), None) //wait for more promises
        else if (isNewer n np)
             then (PWaiting np, None) //if it is newer we cancel session and up the n to the newest
             else (PPrepareSent (n, v, promises), None) //it was an old one. Ignore
      | MAccepted _ -> (s, None) //it was an old one :-). Ignore
    | PAcceptSent (n, v, accepts) ->
      match m with
      | MPromise _ -> (s, None) //either from another session, or a promise that was not needed for this one. Ignore
      | MAccepted (sender, np, v) ->
        if (eq n np) //it is an accept in this session initiated by me
        then let accepts' = (sender, n, v) :: accepts
             if ((isQuorum accepts') && (isAgreement v accepts'))
             then (PLeader n, None)
             else (PAcceptSent (n, v, accepts), None)
        else if (isNewer n np)
             then (PWaiting np, None) //if it is newer we cancel session and up the n to the newest
             else (PPrepareSent (n, v, accepts), None) //it was an old one. Ignore
    | PLeader n ->
      match m with
      | MPromise (sender, n, v) -> (s,None) 
      | MAccepted (sender, n, v) -> (s,None) 

    

  let proposerInitiateLeaderElection (s:PState) (v:V) (sender:Sender): (PState * PMsg Option) =
    match s with
      | PWaiting n -> let n' = inc(n)
                      (PPrepareSent (n',v,[]), 
                       Some (MPrepare(sender, n', v))) 
      | _ -> (s,None)
