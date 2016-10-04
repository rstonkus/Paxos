namespace Paxos

module Queue = 
  type 'a queue = Empty | Single of 'a | Append of 'a queue * 'a queue

  let empty = Empty
  let snoc x xs = Append (xs,Single x)
  let cons x xs = Append (Single x, xs)

  let rec isEmpty xs =
    match xs with
    | Empty -> true
    | Single _ -> false
    | Append (ys,zs) -> (isEmpty ys) && (isEmpty zs)

//  let rec normalize xs =
//    match xs with
//    | Empty -> Empty
//    | Single x -> Single x
//    | Append (ys,zs) -> 
//      match normalize ys with
//      | Empty -> normalize zs
//      | ys' ->
//        match normalize zs with
//        | Empty -> Empty
//        | zs' -> Append (ys',zs')

  //will preserve at most 1 empty in the list
  let rec head xs =
    match xs with
    | Empty -> None
    | Single x -> Some (x,Empty)
    | Append (ys,zs) ->
      match head ys with
      | None -> head zs
      | Some (y,ys') -> 
        match ys' with 
        | Empty -> Some (y, zs)
        | Single y' -> Some (y, Append (Single y',zs))
        | Append (ys'l,ys'r) -> Some (y, Append (Append (ys'l,ys'r), zs))
  
  let tail xs =
    match head xs with 
    | None -> failwith "" 
    | Some (_,xs) -> xs
  