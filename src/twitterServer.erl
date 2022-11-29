-module(twitterServer).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-export([register_user/2,disconnect_user/1,add_follower/2,add_mentions/2,add_subscriber/2,
  add_Tweets/2,get_follower/1,get_my_Tweets/1,get_subscribed_to/1,add_hastags/2]).

-export([get_follower_by_user/2,get_most_subscribed_users/1,loop_hastags/3,loop_mentions/3]).

-define(SERVER, ?MODULE).

-record(twitterServer_state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #twitterServer_state{}} | {ok, State :: #twitterServer_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).

init([]) ->
  %%ETS Table -> Client registry, Tweets, hashtag_mentions, follower, subscriberto
  ets:new(clients_registry, [set, public, named_table]),
  ets:new(tweets, [set, public, named_table]),
  ets:new(hashtags_mentions, [set, public, named_table]),
  ets:new(subscribed_to, [set, public, named_table]),
  ets:new(followers, [set, public, named_table]),
  {ok, #twitterServer_state{}}.

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #twitterServer_state{}) ->
  {reply, Reply :: term(), NewState :: #twitterServer_state{}} |
  {reply, Reply :: term(), NewState :: #twitterServer_state{}, timeout() | hibernate} |
  {noreply, NewState :: #twitterServer_state{}} |
  {noreply, NewState :: #twitterServer_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #twitterServer_state{}} |
  {stop, Reason :: term(), NewState :: #twitterServer_state{}}).
handle_call(_Request, _From, State = #twitterServer_state{}) ->
  {reply, ok, State}.

%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #twitterServer_state{}) ->
  {noreply, NewState :: #twitterServer_state{}} |
  {noreply, NewState :: #twitterServer_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #twitterServer_state{}}).

handle_cast(_Request, State = #twitterServer_state{}) ->
  {noreply, State}.

%% @pgitrivate
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #twitterServer_state{}) ->
  {noreply, NewState :: #twitterServer_state{}} |
  {noreply, NewState :: #twitterServer_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #twitterServer_state{}}).

handle_info(_Info, State = #twitterServer_state{}) ->
  {noreply, State}.

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

%%registering the User
register_user(UserId,Pid) ->
  ets:insert(clients_registry, {UserId, Pid}),
  ets:insert(tweets, {UserId, []}),
  ets:insert(subscribed_to, {UserId, []}),
  FollowerTuple = ets:lookup(followers, UserId),
  if FollowerTuple == [] ->
    ets:insert(followers, {UserId, []});
  true->{}
  end.

%%Disconnect the User
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


%% Setter Functions - to set the subscriber, follower and processing the Tweets
add_subscriber(UserId,Sub)->
  [Tup] = ets:lookup(subscribed_to, UserId),
  List = [Sub | Tup],
  ets:insert(subscribed_to, {UserId, List}).

add_follower(UserId,Sub)->
  [Tup] = ets:lookup(subscribed_to, UserId),
  List = [Sub | Tup],
  ets:insert(subscribed_to, {UserId, List}).


%helper Function
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
  HashtagList= re:compile("~r/\B#[a-zA-Z0-9_]+/",Tweets),
  loop_hastags([HashtagList],UserId, Tweets),
  %% Mentions list
  MentionList = re:compile("~r/\B@[a-zA-Z0-9_]+/",Tweets),
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