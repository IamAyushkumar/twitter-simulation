-module(twitterServer).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-export([register_user/2,disconnect_user/1,add_follower/2,add_mentions/3,add_subscriber/2,
  add_tweets/2,get_follower/1,get_my_tweets/1,get_subscribed_to/1, already_follows/2, is_user_online/1, set_user_online/1, take_user_offine/1]).

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
  %%ETS Table -> Client registry, tweets, hashtag_mentions, follower, subscriberto
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
handle_call({registerUser, Uid, Pid}, _From, State) ->
  DoesAlreadyFollow = register_user(Uid, Pid),
  if DoesAlreadyFollow == false -> {reply, {alreadyRegistered, Uid}, State};
    true ->
      {reply, registerationSuccess, State}
  end.

register_user(Uid, Pid) ->
  ets:insert(clients_registry, {Uid, Pid}),
  ets:insert(tweets, {Uid, []}),
  ets:insert(subscribed_to, {Uid, []}),
  FollowerTuple = ets:lookup(followers, Uid),
  if FollowerTuple == [] ->
    ets:insert(followers, {Uid, []}),
    {true};
  true->{false}
  end.

%%Disconnect the User
disconnect_user(userId)->
  ets:insert(clients_registry,{userId,"NULL"}).

%%   Getter Functions - for getting the tweets, subscriber and follower
get_my_tweets(userId)->
  TweetsTuple = ets:lookup(tweets,userId),
  {TweetsTuple}.

get_subscribed_to(userId)->
  SubscriberTuple=ets:lookup(subscribed_to,userId),
  {SubscriberTuple}.

get_follower(userId)->
  FollowerTuple=ets:lookup(subscribed_to,userId),
  {FollowerTuple}.


%% Setter Functions - to set the subscriber, follower and processing the tweets
add_subscriber(userId,sub)->
  [Tup] = ets:lookup(subscribed_to, userId),
  List = element(Tup, 1),
  List = [sub | List],
  ets:insert(subscribed_to, {userId, List}).

add_follower(userId,sub)->
  [Tup] = ets:lookup(subscribed_to, userId),
  List = element(Tup, 1),
  List = [sub | List],
  ets:insert(subscribed_to, {userId, List}).


%helper Function
loop_hastags([_|tag],userId, tweets)->
  add_hastags(_,userId,tweet),
  loop_hashtag(tag, userId, tweet).

loop_mentions([_|mentions],userId, tweets)->
  add_hastags(_,userId,tweet),
  loop_mentions(mentions, userId, tweet).

%% Processing the Tweets
add_tweets(userId, tweet)->
  TweetsTuple=ets:lookup(tweet, userId),
  ets:insert(tweet,{userId,[TweetsTuple|tweet]}),
  %% Hashtag list
  HashtagList= re:compile("~r/\B#[a-zA-Z0-9_]+/",tweet),
  loop_hastags([HashtagList],userId, tweets),
  %% Mentions list
  MentionList = re:compile("~r/\B@[a-zA-Z0-9_]+/",tweet),
  loop_hastags([MentionList],userId, tweets).

add_hastags(tag, userId, tweets)->
  HashtagTuple=ets:lookup(hashtags_mentions, tag),
  AddHashtag=[HashtagTuple| tag],
  ets:insert(hashtags_mentions,{userId,AddHashtag}).

add_mentions(mentions,userId, tweets)->
  MentionsTuple=ets:lookup(hashtags_mentions, tag),
  AddMentions=[MentionsTuple| tag],
  ets:insert(hashtags_mentions,{userId,AddMentions}).

%%ZIPf Distribution helper functions
count_subscriber(follower) -> length([X || X <- follower, X < 1]).

get_follower_by_user([First |userIdList], ZipfUserMap)->
  maps:put(userId, count_subscriber(get_follower(First, ZipfUserMap))),
  get_follower_by_user([userIdList], ZipfUserMap).

get_most_subscribed_users(userIdList)->
  %%Map where user Id is sorted according to the count of their subscribers
  ZipfUserMap = #{},
  get_follower_by_user(userIdList,[ZipfUserMap]),
  %% Map to List, for sorting on the value of map ( no. of follower)
  List = maps:to_list(ZipfUserMap),
  lists:keysort(2, List),
  {List}.

is_user_online(UserId) -> {}.

set_user_online(UserId) -> {}.

take_user_offine(UserId) -> {}.

already_follows(Celeb, Follower) -> {}. %% already follows? true/false atom.
