% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_file).
-behaviour(gen_server).

-include("couch_db.hrl").

-define(SIZE_BLOCK, 4096).

-record(file, {
    fd,
    client
    }).

-export([open/1, open/2, close/1, sync/1, append_binary/2]).
-export([append_term/2, pread_term/2, pread_iolist/2, write_header/2]).
-export([read_header/1, truncate/2]).
-export([append_term_md5/2, delete/2]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2, code_change/3, handle_info/2]).

%%----------------------------------------------------------------------
%% Args:   Valid Options are [create] and [create,overwrite].
%%  Files are opened in read/write mode.
%% Returns: On success, {ok, Fd}
%%  or {error, Reason} if the file could not be opened.
%%----------------------------------------------------------------------

open(Filepath) ->
    open(Filepath, []).

open(Filepath, _Options) ->
    case gen_server:start_link(couch_file, {Filepath}, []) of
    {ok, Fd} ->
        {ok, Fd};
    Error ->
        Error
    end.


%%----------------------------------------------------------------------
%% Purpose: To append an Erlang term to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos} where Pos is the file offset to the beginning the
%%  serialized  term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_term(Fd, Term) ->
    append_binary(Fd, Term).
    
append_term_md5(Fd, Term) ->
    append_binary(Fd, Term).

delete(_Dir, _File) ->
  ok.  % eventually have riak drop DBs we're trying to delete

%%----------------------------------------------------------------------
%% Purpose: To append an Erlang binary to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos} where Pos is the file offset to the beginning the
%%  serialized  term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_binary(Fd, Bin) ->
    gen_server:call(Fd, {append_bin, Bin}, infinity).
    
%%----------------------------------------------------------------------
%% Purpose: Reads a term from a file that was written with append_term
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------


pread_term(Fd, Pos) ->
    gen_server:call(Fd, {pread_iolist, Pos}, infinity).

pread_iolist(Fd, Pos) ->
    gen_server:call(Fd, {pread_iolist, Pos}, infinity).

truncate(Fd, Pos) ->
    gen_server:call(Fd, {truncate, Pos}, infinity).

%%----------------------------------------------------------------------
%% Purpose: Ensure all bytes written to the file are flushed to disk.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------

sync(Filepath) when is_list(Filepath) ->
  ok;
sync(Fd) ->
    gen_server:call(Fd, sync, infinity).

%%----------------------------------------------------------------------
%% Purpose: Close the file.
%% Returns: ok
%%----------------------------------------------------------------------
close(Fd) ->
    MRef = erlang:monitor(process, Fd),
    try
        catch unlink(Fd),
        catch exit(Fd, shutdown),
        receive
        {'DOWN', MRef, _, _, _} ->
            ok
        end
    after
        erlang:demonitor(MRef, [flush])
    end.


read_header(Fd) ->
    case gen_server:call(Fd, find_header, infinity) of
    {ok, Bin} ->
        {ok, Bin};
    Else ->
        Else
    end.

write_header(Fd, Data) ->
    gen_server:call(Fd, {write_header, Data}, infinity).


% server functions

init({Filepath}) when is_list(Filepath) ->
    % io:format("Filepath is: ~p~n", [Filepath]),
    process_flag(trap_exit, true),
    {ok, Client} = riak:client_connect('riak@127.0.0.1'),
    BinFile = list_to_binary(filename:basename(Filepath)),
    % io:format("Using database ~p~n", [BinFile]),
    {ok, #file{fd = BinFile, client = Client}}.

terminate(_Reason, _Fd) ->
    ok.


handle_call({pread_iolist, Pos}, _From, #file{fd = Fd, client = Client} = File) when is_binary(Pos) ->
    % io:format("Trying to read ~p~n", [Pos]),
    case Client:get(Fd, Pos, 1) of
      {ok, Got} -> Value = riak_object:get_value(Got),
                   % io:format("Got value: ~p~n", [Value]),
                   {reply, {ok, Value}, File};
      {error, notfound} -> io:format("Didn't find Pos ~p~n", [Pos]),
                           {reply, {ok, <<>>}, File}
    end;
handle_call({truncate, 0}, _From, #file{fd=Fd}=File) ->
    io:format("Request to delete FD (not acting on it): ~p~n", [Fd]),
    {reply, ok, File};
handle_call({append_bin, Bin}, _From, #file{fd=Fd, client = Client}=File) ->
    NewPos = term_to_binary({node(), now()}),
    % io:format("Trying to write: ~p = ~p~n", [NewPos, Bin]),
    New = riak_object:new(Fd, NewPos, Bin),
    ok = Client:put(New, 1),
    {reply, {ok, NewPos}, File};
handle_call({write_header, Bin}, _From, #file{fd=Fd, client=Client}=File) ->
    NewPos = <<"header">>,
    New = riak_object:new(Fd, NewPos, Bin),
    Client:put(New, 1),
    {reply, ok, File};

handle_call(find_header, _From, #file{fd = Fd, client = Client} = File) ->
    Pos = <<"header">>,
    % io:format("Asking for header for fd: ~p~n", [Fd]),
    case Client:get(Fd, Pos, 1) of
      {ok, Got} -> {reply, {ok, riak_object:get_value(Got)}, File};
      {error, notfound} -> {reply, no_valid_header, File}
    end.

handle_cast(close, Fd) ->
    {stop,normal,Fd}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info({'EXIT', _, normal}, Fd) ->
    {noreply, Fd};
handle_info({'EXIT', _, Reason}, Fd) ->
    {stop, Reason, Fd}.

