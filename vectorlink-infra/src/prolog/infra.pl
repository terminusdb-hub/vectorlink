:- module(infra, [
              clear_database/0,
              load_configuration/1
          ]).

:- dynamic machine/1.
:- dynamic machine_type/2.
:- dynamic machine_state/2.

clear_database :-
    retractall(machine(_)),
    retractall(machine_type(_,_)),
    retractall(machine_state(_,_)),
    true.

unload_machine(Id) :-
    retractall(machine(Id)),
    retractall(machine_type(Id,_)),
    retractall(machine_state(Id,_)),
    true.

load_machine(Dict) :-
    _{ id: Id, type: Type} :< Dict,
    unload_machine(Id),
    assertz(machine(Id)),
    assertz(machine_type(Id,Type)).

load_machines([]).
load_machines([Machine|Machines]) :-
    load_machine(Machine),
    load_machines(Machines).

load_configuration(Dict) :-
    (   get_dict(machines, Dict, Machines)
    ->  load_machines(Machines)
    ;   true).


op_dif(Aws_State, Desired_State, Diff) :-
    true.
