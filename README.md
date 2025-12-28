(*
  VeriFlow
  Deterministic Dataflow Engine with Static Guarantees
  OCaml 5.x
*)

module StringMap = Map.Make(String)

(* =========================
   Core Types
   ========================= *)

type value =
  | Int of int
  | Str of string

type node_id = string

type operation =
  | Add
  | Mul
  | Concat

type node = {
  id : node_id;
  op : operation;
  inputs : node_id list;
}

type graph = node list

type env = value StringMap.t

exception CycleDetected of string
exception MissingDependency of string
exception TypeError of string

(* =========================
   Utility
   ========================= *)

let string_of_value = function
  | Int i -> string_of_int i
  | Str s -> s

let hash_value = function
  | Int i -> Hashtbl.hash i
  | Str s -> Hashtbl.hash s

(* =========================
   Topological Sort
   ========================= *)

let topo_sort (g : graph) =
  let visited = Hashtbl.create 32 in
  let stack = Hashtbl.create 32 in
  let result = ref [] in

  let rec visit n =
    if Hashtbl.mem stack n.id then
      raise (CycleDetected n.id);

    if not (Hashtbl.mem visited n.id) then begin
      Hashtbl.add stack n.id true;
      List.iter
        (fun dep ->
          match List.find_opt (fun x -> x.id = dep) g with
          | Some d -> visit d
          | None -> raise (MissingDependency dep)
        )
        n.inputs;
      Hashtbl.remove stack n.id;
      Hashtbl.add visited n.id true;
      result := n :: !result
    end
  in

  List.iter visit g;
  List.rev !result

(* =========================
   Execution
   ========================= *)

let eval_op op args =
  match op, args with
  | Add, [Int a; Int b] -> Int (a + b)
  | Mul, [Int a; Int b] -> Int (a * b)
  | Concat, [Str a; Str b] -> Str (a ^ b)
  | _ -> raise (TypeError "Invalid operation/argument types")

let execute (g : graph) (inputs : env) =
  let ordered = topo_sort g in
  let env_ref = ref inputs in
  let cost = ref 0 in

  List.iter
    (fun node ->
      let args =
        List.map
          (fun dep ->
            match StringMap.find_opt dep !env_ref with
            | Some v -> v
            | None -> raise (MissingDependency dep)
          )
          node.inputs
      in
      let result = eval_op node.op args in
      incr cost;
      env_ref := StringMap.add node.id result !env_ref
    )
    ordered;

  (!env_ref, !cost)

(* =========================
   Audit
   ========================= *)

let fingerprint env =
  StringMap.fold
    (fun k v acc -> acc lxor (Hashtbl.hash k lxor hash_value v))
    env
    0

(* =========================
   Demo
   ========================= *)

let () =
  let graph = [
    { id = "a"; op = Add; inputs = ["x"; "y"] };
    { id = "b"; op = Mul; inputs = ["a"; "z"] };
    { id = "c"; op = Concat; inputs = ["s1"; "s2"] };
  ] in

  let inputs =
    StringMap.empty
    |> StringMap.add "x" (Int 2)
    |> StringMap.add "y" (Int 3)
    |> StringMap.add "z" (Int 4)
    |> StringMap.add "s1" (Str "hello ")
    |> StringMap.add "s2" (Str "world")
  in

  let (final_env, cost) = execute graph inputs in
  let fp = fingerprint final_env in

  print_endline "Final values:";
  StringMap.iter
    (fun k v -> Printf.printf "%s = %s\n" k (string_of_value v))
    final_env;

  Printf.printf "Execution cost: %d\n" cost;
  Printf.printf "Fingerprint: %d\n" fp
