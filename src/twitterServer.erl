
-module(twitterServer).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, terminate/2, code_change/3]).


-export([register_user/2,disconnect_user/1,add_follower/2,add_mentions/2,add_subscriber/2,
  add_Tweets/2,get_follower/1,get_my_Tweets/1,get_subscribed_to/1,add_hastags/2, is_user_online/1, set_user_online/1,take_user_offine/1,already_follows/2]).

-export([get_follower_by_user/2,get_most_subscribed_users/1,loop_hastags/3,loop_mentions/3]).

-define(SERVER, ?MODULE).

-record(twitterServer_state, {uid, pid, subcribers, tag, tweets, mentions, useridlist, followerid}).
%% Here tweets is tweet Id and mentions is mentions id and sub is Subscriber Id

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []),
  {ok, self()}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #twitterServer_state{}} | {ok, State :: #twitterServer_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).

init([]) ->
  %%ETS Table -> Client registry, Tweets, hashtag_mentions, follower, subscribed_to
  ets:new(clients_registry, [set, public, named_table]),
  ets:new(tweets, [set, public, named_table]),
  ets:new(hashtags_mentions, [set, public, named_table]),
  ets:new(subscribed_to, [set, public, named_table]),
  ets:new(followers, [set, public, named_table]),
  ets:new(active_user, [set, public, named_table]),
  {ok, #twitterServer_state{}}.

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #twitterServer_state{}) ->
  {reply, Reply :: term(), NewState :: #twitterServer_state{}} |
  {reply, Reply :: term(), NewState :: #twitterServer_state{}, timeout() | hibernate}|
  {reply, Reply :: term(), NewState :: #twitterServer_state{}} |
  {reply, Reply :: term(), NewState :: #twitterServer_state{}, timeout() | hibernate}|
  {reply, Reply :: term(), NewState :: #twitterServer_state{}} |
  {reply, Reply :: term(), NewState :: #twitterServer_state{}, timeout() | hibernate}|
  {reply, Reply :: term(), NewState :: #twitterServer_state{}} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #twitterServer_state{}} |
  {stop, Reason :: term(), NewState :: #twitterServer_state{}}).

%% For registering the User
handle_call({UserId, Pid}, _From, State = #twitterServer_state{uid=UserId, pid=Pid}) ->
  register_user(uid),
  {reply, ok, State}.
%% For adding the follower
handle_call({UserId, Sub}, _From, State = #twitterServer_state{uid=UserId, subcribers = Sub}) ->
  add_follower(uid,subcribers),
  {reply, ok, State}.
%% For adding the tweets of the given User
handle_call({UserId, Tweets}, _From, State = #twitterServer_state{uid=UserId, tweets =Tweets}) ->
  add_Tweets(uid, tweets),
  {reply, ok, State}.
%% For adding in the hashtags tuple corresponding to the user
handle_call({UserId, Tag}, _From, State = #twitterServer_state{uid=UserId, tag =Tag}) ->
  add_hastags(tag, uid),
  {reply, ok, State}.
%% For adding in the mentions corresponding to the user
handle_call({UserId, Mentions}, _From, State = #twitterServer_state{uid=UserId, mentions = Mentions}) ->
  add_mentions(mentions,uid),
  {reply, ok, State}.
%% setting the user active
handle_call({UserId}, _From, State = #twitterServer_state{uid=UserId}) ->
  set_user_online(uid),
  {reply, ok, State}.
%% setting the user inactive
handle_call({UserId}, _From, State = #twitterServer_state{uid=UserId}) ->
  take_user_offine(uid),
  {reply, ok, State}.


%% @private
%% @doc Handling call messages
%% Dividing User actor is divided in 2 sets -
%% 1. set  - Tweet Actor( for registering tweets )  , Mention Actor, Hashtag Actor,
%% 2. Get  - Mention Actor, Hashtag Actor, Retweet Actors
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #twitterServer_state{}) ->
  {noreply, NewState :: #twitterServer_state{}} |
  {noreply, NewState :: #twitterServer_state{}, timeout() | hibernate} |
  {noreply, NewState :: #twitterServer_state{}} |
  {noreply, NewState :: #twitterServer_state{}, timeout() | hibernate} |
  {noreply, NewState :: #twitterServer_state{}} |
  {noreply, NewState :: #twitterServer_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #twitterServer_state{}} |
  {stop, Reason :: term(), NewState :: #twitterServer_state{}}).
%%get the number of subscriber
handle_call({UserId}, _From, State = #twitterServer_state{uid=UserId}) ->
  get_subscribed_to(uid),
  {reply, ok, State}.
%%get the number of follower
handle_call({UserId}, _From, State = #twitterServer_state{uid=UserId}) ->
  get_follower(uid),
  {reply, ok, State}.
%%get the number of tweets
handle_call({UserId}, _From, State = #twitterServer_state{uid=UserId}) ->
  get_my_Tweets(uid),
  {reply, ok, State}.
%% get the user List for ZIPf distribution
handle_call({UserIdList}, _From, State = #twitterServer_state{ useridlist =UserIdList}) ->
  get_most_subscribed_users(useridlist),
  {reply, ok, State}.
%% checking the user follows or not
handle_call({UserId, FollowerId}, _From, State = #twitterServer_state{uid=UserId, followerid=FollowerId}) ->
  already_follows(uid, followerid),
  {reply, ok, State}.
%% is user online ?
handle_call({UserId}, _From, State = #twitterServer_state{uid=UserId}) ->
  is_user_online(uid),
  {reply, ok, State}.

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #twitterServer_state{}) -> term()).
terminate(_Reason, _State = #twitterServer_state{}) ->
  ok.

%% @private
%% @doc Convert process state when code is changed
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #twitterServer_state{},
    Extra :: term()) ->
  {ok, NewState :: #twitterServer_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #twitterServer_state{}, _Extra) ->
  {ok, State}.


%%%===================================================================
%%% Internal functions - Database Handler
%%%===================================================================

register_user(UserId,Pid) ->
  ets:insert(clients_registry, {UserId, Pid}),
  ets:insert(tweets, {UserId, []}),
  ets:insert(subscribed_to, {UserId, []}),
  ets:insert(active_user, {UserId, #{}}),
  FollowerTuple = ets:lookup(followers, UserId),
  if FollowerTuple == [] ->
    ets:insert(followers, {UserId, []});
    true->{}
  end.

%%   Disconnect the User
disconnect_user(UserId)->
  ets:insert(clients_registry,{UserId,"NULL"}).

%%   Getter Functions - for getting the Tweets, subscriber and follower
get_my_Tweets(UserId)->
  TweetsTuple = ets:lookup(tweets,UserId),
  {TweetsTuple}.

get_subscribed_to(UserId)->
  SubscriberTuple=ets:lookup(subscribed_to,UserId),
  {SubscriberTuple}.

get_follower(UserId)->
  FollowerTuple = ets:lookup(subscribed_to,UserId),
  {FollowerTuple}.


%%  Setter Functions - to set the subscriber, follower and processing the Tweets
add_subscriber(UserId,Sub)->
  [Tup] = ets:lookup(subscribed_to, UserId),
  List = [Sub | Tup],
  ets:insert(subscribed_to, {UserId, List}).

add_follower(UserId,Sub)->
  [Tup] = ets:lookup(subscribed_to, UserId),
  List = [Sub | Tup],
  ets:insert(subscribed_to, {UserId, List}).


%%helper Function
loop_hastags([_|Tag],UserId, Tweets)->
  add_hastags(_,UserId),
  loop_hastags(Tag, UserId, Tweets).

loop_mentions([_|Mentions],UserId, Tweets)->
  add_hastags(_,UserId),
  loop_hastags(Mentions, UserId, Tweets).

%% Processing the Tweets
add_Tweets(UserId, Tweets)->
  TweetsTuple=ets:lookup(tweet, UserId),
  ets:insert(tweet,{UserId,[TweetsTuple|Tweets]}),
  %% Hashtag list
  HashtagList= re:compile("~r#[a-zA-Z0-9_]+",Tweets),
  loop_hastags([HashtagList],UserId, Tweets),
  %% Mentions list
  MentionList = re:compile("~r@[a-zA-Z0-9_]+",Tweets),
  loop_hastags([MentionList],UserId, Tweets).

add_hastags(Tag, UserId)->
  HashtagTuple=ets:lookup(hashtags_mentions, Tag),
  AddHashtag=[HashtagTuple| Tag],
  ets:insert(hashtags_mentions,{UserId,AddHashtag}).

add_mentions(Mentions,UserId)->
  MentionsTuple=ets:lookup(hashtags_mentions, Mentions),
  AddMentions=[MentionsTuple| Mentions],
  ets:insert(hashtags_mentions,{UserId,AddMentions}).

%%ZIPf Distribution helper functions
count_subscriber(Follower) -> length([X || X <- Follower, X < 1]).

get_follower_by_user([First |UserIdList], ZipfUserMap)->
  maps:put(First, count_subscriber(get_follower(First)), ZipfUserMap),
  get_follower_by_user([UserIdList], ZipfUserMap).

get_most_subscribed_users(UserIdList)->
  %%Map where user Id is sorted according to the count of their subscribers
  ZipfUserMap = #{},
  get_follower_by_user(UserIdList,[ZipfUserMap]),
  %% Map to List, for sorting on the value of map ( no. of follower)
  List = maps:to_list(ZipfUserMap),
  lists:keysort(2, List),
  {List}.

is_user_online(UserId) ->
  ActiveUserMap = ets:lookup(active_user,UserId),
  {maps:get(UserId, ActiveUserMap)}.

set_user_online(UserId) ->
  ActiveUserMap = ets:lookup(active_user,UserId),
  maps:put(UserId, 1, ActiveUserMap).


take_user_offine(UserId) ->
  ActiveUserMap = ets:lookup(active_user,UserId),
  maps:put(UserId, 0, ActiveUserMap).


%% finding in the follower tuple
find_in_follower_tuple(_, []) -> false;

find_in_follower_tuple(E, T) when is_tuple(T) ->
  find_in_follower_tuple(E, tuple_to_list(T));

find_in_follower_tuple(E, [H|T]) ->
  case find_in_follower_tuple(E, H) of
    false -> find_in_follower_tuple(E, T);
    true -> true
  end.

already_follows(Celeb, Follower) ->
  FollowerTuple = get_follower(Celeb),
  find_in_follower_tuple(FollowerTuple,Follower).