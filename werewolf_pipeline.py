import hashlib
import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window


# Variable-shaped event payloads retain structured data and omit bulky text.
# No schema inference, external Python packages, or driver-side collection.
TYPES = {
    "s": T.StringType(), "l": T.LongType(), "d": T.DoubleType(),
    "b": T.BooleanType(), "as": T.ArrayType(T.StringType()),
}


def schema(fields):
    return T.StructType([
        T.StructField(name, TYPES[kind], True)
        for name, kind in (field.split(":") for field in fields.split())
    ])


SCHEMAS = {
    "episodes": schema("""
        episode_id:l uuid:s name:s title:s game_version:s module_version:s
        schema_version:l seed:l configuration_json:s winner_team:s
        final_day:l final_phase:s terminated_with_agent_error:b
        source_path:s source_sha256:s source_bytes:l
    """),
    "players": schema("""
        episode_id:l player_id:s seat_index:l model_name:s role:s team:s
        role_params_json:s is_alive_final:b eliminated_during_day:l
        eliminated_during_phase:s score:d is_winner:b
    """),
    "actions": schema("""
        episode_id:l step_index:l seat_index:l actor_id:s action_type:s
        day:l phase:s target_id:s message:s reasoning:s
        perceived_threat_level:s prompt_tokens:l completion_tokens:l
        reported_cost:d error:s kwargs_json:s source_path:s source_sha256:s
        raw_prompt_pointer:s raw_completion_pointer:s
    """),
    "events": schema("""
        episode_id:l batch_index:l entry_index:l data_type:s event_name:s
        source:s created_at_raw:s day:l phase:s detailed_phase:s
        public:b visible_to:as visible_in_ui:b description:s
        data_json:s
    """),
}

PARSED_SCHEMA = T.StructType([
    T.StructField("episode_id", T.LongType(), False),
    *[T.StructField(name, T.ArrayType(row_schema), False)
      for name, row_schema in SCHEMAS.items()],
])


def canonical(value):
    # Versioned application convention: recursive key sorting, UTF-8,
    # compact separators, preserved array order, no non-finite numbers.
    # This is not a claim of RFC 8785 canonicalization.
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False)


EVENT_PAYLOAD_TEXT_FIELDS = frozenset({
    "raw_prompt", "raw_completion", "reasoning", "action_json_schema",
    "rule_of_role", "discussion_rule", "voting_protocol_rule",
    "day_discussion_protocol_rule", "night_werewolf_discussion_protocol_rule",
    "day_voting_protocol_rule", "night_werewolf_voting_protocol_rule",
})
ACTION_TELEMETRY_EVENTS = frozenset({
    "discussion", "vote_action", "eliminate_proposal_action",
    "heal_action", "inspect_action", "no_op_action",
})


def compact_event_payload(value):
    """Preserve structural values and unknown fields; omit named audit text.

    Apply recursively so nested objects cannot reintroduce these text fields.
    Error details remain in actions/the source archive; events retain a flag.
    """
    if isinstance(value, list):
        return [compact_event_payload(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {
        key: compact_event_payload(item)
        for key, item in value.items()
        if key not in EVENT_PAYLOAD_TEXT_FIELDS and key != "error"
    }
    if "error" in value:
        result["has_error"] = bool(value["error"])
    return result


def retained_event_description(data_type, event):
    """Keep narrative events, including requests whose data is null."""
    data = event.get("data")
    telemetry = (
        data_type == "dict" and event.get("event_name") in ACTION_TELEMETRY_EVENTS
    ) or (
        isinstance(data, dict)
        and ("raw_prompt" in data or "raw_completion" in data)
    )
    return None if telemetry else event.get("description")


def reject_constant(value):
    raise ValueError(f"Non-JSON numeric constant: {value}")


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON object key: {key}")
        result[key] = value
    return result


def load_json(text):
    return json.loads(text, parse_constant=reject_constant,
                      object_pairs_hook=unique_keys)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def as_double(value):
    return None if value is None else float(value)


def parse_episode(content, source_path):
    try:
        return normalize_episode(bytes(content), source_path)
    except Exception as exc:
        raise ValueError(f"{source_path}: {exc}") from exc


def normalize_episode(content, source_path):
    root = load_json(content.decode("utf-8-sig"))
    require(root["schema_version"] == 1, "Unsupported schema_version")
    episode_id = root["info"]["EpisodeId"]
    require(type(episode_id) is int, "EpisodeId must be an integer")
    end = root["info"]["GAME_END"]  # Completed episodes are required.
    config = root["configuration"]
    steps = root["steps"]
    roster = end["all_players"]
    require(bool(steps) and bool(roster), "Missing steps or final roster")
    source_hash = hashlib.sha256(content).hexdigest()
    output = {name: [] for name in SCHEMAS}
    output["episode_id"] = episode_id

    def emit(table, **values):
        values["episode_id"] = episode_id
        output[table].append(values)

    # Resolve actual identities from observations, never configured names.
    seats, identities = {}, {}
    for step_index, batch in enumerate(steps):
        require(len(batch) == len(roster),
                f"step {step_index}: unexpected seat count")
        for seat_index, record in enumerate(batch):
            raw = record.get("observation", {}).get("raw_observation")
            if raw is None:
                continue
            player_id = raw["player_id"]
            identity = (raw["role"], raw["team"])
            require(seats.get(seat_index, player_id) == player_id,
                    f"seat {seat_index}: player identity changed")
            require(identities.get(player_id, identity) == identity,
                    f"player {player_id}: role/team changed")
            seats[seat_index] = player_id
            identities[player_id] = identity

    roster_ids = [player["id"] for player in roster]
    require(len(set(roster_ids)) == len(roster_ids), "Duplicate roster ID")
    require(len(seats) == len(roster)
            and set(seats.values()) == set(roster_ids),
            "Cannot resolve a unique seat for every roster player")
    seat_by_player = {player_id: seat for seat, player_id in seats.items()}
    require(len(root["rewards"]) == len(roster), "Reward count mismatch")

    emit("episodes", uuid=root["id"], name=root.get("name"),
         title=root.get("title"), game_version=root.get("version"),
         module_version=root.get("module_version"),
         schema_version=root["schema_version"], seed=config.get("seed"),
         configuration_json=canonical(config),
         winner_team=end["winner_team"], final_day=end["last_day"],
         final_phase=end["last_phase"],
         terminated_with_agent_error=end.get("terminated_with_agent_error"),
         source_path=source_path, source_sha256=source_hash,
         source_bytes=len(content))

    for player in roster:
        player_id = player["id"]
        agent = player["agent"]
        role, team = identities[player_id]
        seat = seat_by_player[player_id]
        require(agent["id"] == player_id and agent["role"] == role
                and end["all_players_and_role"][player_id] == role,
                f"player {player_id}: final role/identity mismatch")
        score = end["scores"][player_id]
        require(score == root["rewards"][seat],
                f"player {player_id}: reward/score mismatch")
        emit("players", player_id=player_id, seat_index=seat,
             model_name=agent["display_name"], role=role, team=team,
             role_params_json=canonical(agent.get("role_params", {})),
             is_alive_final=player["alive"],
             eliminated_during_day=player["eliminated_during_day"],
             eliminated_during_phase=player["eliminated_during_phase"],
             score=as_double(score), is_winner=player_id in end["winner_ids"])

    for step_index, batch in enumerate(steps):
        for seat_index, record in enumerate(batch):
            action = record.get("action")
            if action is None or action == {}:
                continue
            require(isinstance(action, dict) and bool(action.get("action_type")),
                    f"step {step_index}, seat {seat_index}: invalid action")
            kwargs = action["kwargs"]
            require(kwargs["actor_id"] == seats[seat_index],
                    f"step {step_index}, seat {seat_index}: actor mismatch")
            pointer = f"/steps/{step_index}/{seat_index}/action/kwargs"
            # Full text stays in the archived source. Pointer existence
            # preserves the distinction between absent and explicit null.
            core_kwargs = {key: value for key, value in kwargs.items()
                           if key not in ("raw_prompt", "raw_completion")}
            emit("actions", step_index=step_index, seat_index=seat_index,
                 actor_id=kwargs["actor_id"], action_type=action["action_type"],
                 day=kwargs.get("day"), phase=kwargs.get("phase"),
                 target_id=kwargs.get("target_id"), message=kwargs.get("message"),
                 reasoning=kwargs.get("reasoning"),
                 perceived_threat_level=kwargs.get("perceived_threat_level"),
                 prompt_tokens=kwargs.get("prompt_tokens"),
                 completion_tokens=kwargs.get("completion_tokens"),
                 reported_cost=as_double(kwargs.get("cost")),
                 error=kwargs.get("error"), kwargs_json=canonical(core_kwargs),
                 source_path=source_path, source_sha256=source_hash,
                 raw_prompt_pointer=(pointer + "/raw_prompt"
                                     if "raw_prompt" in kwargs else None),
                 raw_completion_pointer=(pointer + "/raw_completion"
                                         if "raw_completion" in kwargs else None))

    batches = root["info"]["MODERATOR_OBSERVATION"]
    require(len(batches) == len(steps), "Moderator/step batch count mismatch")
    for batch_index, batch in enumerate(batches):
        for entry_index, wrapper in enumerate(batch):
            coordinate = f"moderator batch {batch_index}, entry {entry_index}"
            try:
                event = load_json(wrapper["json_str"])
                require(isinstance(event, dict), "Event must be an object")
                if event["event_name"] == "game_start":
                    data = event["data"]
                    require(identities[data["player_id"]]
                            == (data["role"], data["team"]),
                            "Initial role/team mismatch")
                emit("events", batch_index=batch_index, entry_index=entry_index,
                     data_type=wrapper["data_type"], event_name=event["event_name"],
                     source=event.get("source"), created_at_raw=event.get("created_at"),
                     day=event.get("day"), phase=event.get("phase"),
                     detailed_phase=event.get("detailed_phase"),
                     public=event.get("public"), visible_to=event.get("visible_to"),
                     visible_in_ui=event.get("visible_in_ui"),
                     description=retained_event_description(wrapper["data_type"], event),
                     data_json=canonical(compact_event_payload(event.get("data"))))
            except Exception as exc:
                raise ValueError(f"{coordinate}: {exc}") from exc
    return output


normalize_udf = F.udf(parse_episode, PARSED_SCHEMA)


@dp.table(
    name="werewolf_json_normalized",
    table_properties={"quality": "bronze"},
    comment="Normalized episode files ingested incrementally by Auto Loader",
)
def werewolf_json_normalized():
    files = (spark.readStream.format("cloudFiles")
             .option("cloudFiles.format", "binaryFile")
             .option("cloudFiles.includeExistingFiles", "true")
             .option("cloudFiles.allowOverwrites", "false")
             .option("pathGlobFilter", "*.json")
             .option("recursiveFileLookup", "true")
             .load(spark.conf.get("werewolf.source_path")))
    parsed = files.select(normalize_udf("content", "path").alias("p")).select("p.*")
    return (parsed
            .withColumn("source_path", F.col("episodes")[0]["source_path"])
            .withColumn("source_sha256", F.col("episodes")[0]["source_sha256"]))


@dp.temporary_view(name="_werewolf_silver_input")
@dp.expect_or_fail("one_source_per_episode", "_episode_source_count = 1")
def werewolf_silver_input():
    # Batch windows belong on the silver side, not in the append stream.
    # Auto Loader tracks file paths; separate copies can still share content.
    bronze = spark.read.table("werewolf_json_normalized")
    copy_window = Window.partitionBy("source_sha256").orderBy("source_path")
    unique_files = (bronze
                    .withColumn("_copy_rank", F.row_number().over(copy_window))
                    .where("_copy_rank = 1").drop("_copy_rank"))
    return unique_files.withColumn(
        "_episode_source_count",
        F.count(F.lit(1)).over(Window.partitionBy("episode_id")),
    )


def records(name):
    return (spark.read.table("_werewolf_silver_input")
            .select(F.explode(F.col(name)).alias("r")).select("r.*"))


@dp.materialized_view(name="silver.episodes", table_properties={"quality": "silver"})
def episodes():
    return records("episodes")


@dp.materialized_view(name="silver.players", table_properties={"quality": "silver"})
def players():
    return records("players").withColumn(
        "is_alive_winner",
        F.coalesce(F.col("is_alive_final") & F.col("is_winner"), F.lit(False)),
    )


@dp.materialized_view(name="silver.actions", table_properties={"quality": "silver"})
def actions():
    return records("actions")


@dp.materialized_view(name="silver.events", table_properties={"quality": "silver"})
@dp.expect_or_fail(
    "valid_event_timestamp",
    "created_at_raw IS NULL OR created_at IS NOT NULL",
)
def events():
    return records("events").withColumn(
        "created_at", F.try_to_timestamp(F.col("created_at_raw"))
    )

GOLD = {"quality": "gold"}
RULE_FIELDS = (
    "day_exile_reveal_level", "night_elimination_reveal_level",
    "day_voting_protocol", "discussion_protocol",
    "werewolf_night_vote_protocol", "randomize_ids", "randomize_roles",
    "episodeSteps", "actTimeout", "runTimeout", "maxLogLength",
)


def source(name):
    # Quote each identifier instead of interpolating SQL text.
    parts = spark.conf.get("werewolf.silver_schema").split(".")
    if len(parts) != 2 or not all(parts):
        raise ValueError("werewolf.silver_schema must be catalog.schema")
    return spark.read.table(".".join(
        "`" + part.replace("`", "``") + "`" for part in [*parts, name]
    ))


def min_games():
    value = int(spark.conf.get("werewolf.gold_min_games", "20"))
    if value < 1:
        raise ValueError("werewolf.gold_min_games must be positive")
    return value


@dp.temporary_view(name="_gold_validated_players")
@dp.expect_all_or_fail({
    "unique_player": "_player_count = 1",
    "unique_episode": "_episode_count = 1",
    "episode_exists": "game_version IS NOT NULL",
    "identity_present": "episode_id IS NOT NULL AND player_id IS NOT NULL "
                        "AND model_name IS NOT NULL AND role IS NOT NULL "
                        "AND team IS NOT NULL",
    "outcome_present": "is_winner IS NOT NULL AND is_alive_final IS NOT NULL",
    "alive_winner_consistent":
        "is_alive_winner IS NOT NULL AND "
        "is_alive_winner = (is_alive_final AND is_winner)",
})
def validated_players():
    players = source("players").withColumn(
        "_player_count", F.count("*").over(
            Window.partitionBy("episode_id", "player_id")))
    episodes = source("episodes").select(
        "episode_id", "game_version", "module_version", "schema_version",
        "configuration_json", "winner_team", "final_phase",
        "terminated_with_agent_error",
    ).withColumn("_episode_count", F.count("*").over(
        Window.partitionBy("episode_id")))
    return players.join(episodes, "episode_id", "left")


@dp.temporary_view(name="_gold_player_results")
@dp.expect_or_fail("winner_matches_faction", "is_winner = (team = winner_team)")
def player_results():
    valid = spark.read.table("_gold_validated_players")
    completed = valid.where(
        (F.col("final_phase") == "Game Over")
        & F.col("winner_team").isin("Villagers", "Werewolves")
        & (F.col("terminated_with_agent_error") == F.lit(False))
    )
    # Role parameters are included; model names and random seed are not rules.
    rosters = completed.groupBy("episode_id").agg(
        F.to_json(F.sort_array(F.collect_list(F.struct(
            "team", "role", "role_params_json"
        )))).alias("roster_rules_json")
    )
    with_roster = completed.join(rosters, "episode_id")
    rules = F.to_json(F.struct(
        "game_version", "module_version", "schema_version", "roster_rules_json",
        *[F.get_json_object("configuration_json", "$." + key).alias(key)
          for key in RULE_FIELDS],
    ), options={"ignoreNullFields": "false"})
    return (with_roster.withColumn("cohort_rules_json", rules)
            .withColumn("cohort_id", F.sha2("cohort_rules_json", 256)))


def player_summary(keys):
    return (spark.read.table("_gold_player_results").groupBy(*keys).agg(
        F.first("cohort_rules_json").alias("cohort_rules_json"),
        F.count("*").alias("player_appearances"),
        F.countDistinct("episode_id").alias("games_played"),
        F.sum(F.col("is_winner").cast("long")).alias("wins"),
        F.sum(F.col("is_alive_final").cast("long")).alias("survivors"),
        F.sum(F.col("is_alive_winner").cast("long")).alias("surviving_wins"),
        F.avg("score").alias("average_score"),
    ).withColumn("losses", F.col("player_appearances") - F.col("wins"))
      .withColumn("win_rate", F.col("wins") / F.col("player_appearances"))
      .withColumn("survival_rate", F.col("survivors") / F.col("player_appearances"))
      .withColumn("surviving_win_rate",
                  F.col("surviving_wins") / F.col("player_appearances"))
      .withColumn("eligible_for_ranking", F.col("games_played") >= min_games()))


@dp.materialized_view(name="gold.model_role_performance", table_properties=GOLD)
def model_role_performance():
    return player_summary(["cohort_id", "model_name", "role", "team"])


@dp.materialized_view(name="gold.model_overall_performance", table_properties=GOLD)
def model_overall_performance():
    keys = ["cohort_id", "model_name"]
    per_episode = (spark.read.table("_gold_player_results")
                   .groupBy(*keys, "episode_id")
                   .agg(F.avg(F.col("is_winner").cast("double"))
                        .alias("episode_win_share"))
                   .groupBy(*keys).agg(F.avg("episode_win_share")
                                      .alias("episode_equal_win_rate")))
    roles = (spark.read.table("gold.model_role_performance")
             .groupBy(*keys).agg(
                 F.avg("win_rate").alias("role_balanced_win_rate"),
                 F.sort_array(F.collect_set("role")).alias("roles_observed"),
                 F.min("games_played").alias("min_games_in_observed_role")))
    return player_summary(keys).join(per_episode, keys).join(roles, keys)


@dp.materialized_view(name="gold.team_episode_results", table_properties=GOLD)
@dp.expect_or_fail("consistent_team_outcomes", "_winner_values = 1")
def team_episode_results():
    return (spark.read.table("_gold_player_results")
            .groupBy("cohort_id", "episode_id", "team").agg(
                F.first("cohort_rules_json").alias("cohort_rules_json"),
                F.count("*").alias("team_size"),
                F.countDistinct("is_winner").alias("_winner_values"),
                F.max(F.col("is_winner").cast("long")).alias("team_win"),
                F.sum(F.col("is_alive_final").cast("long")).alias("survivors"),
                F.sort_array(F.collect_set("model_name")).alias("models"),
                F.to_json(F.sort_array(F.collect_list(F.struct(
                    "model_name", "role"
                )))).alias("lineup_json"),
            ).withColumn("lineup_id", F.sha2("lineup_json", 256)))


def team_summary(frame, keys):
    # Input must contain at most one row per keys + episode_id.
    return (frame.groupBy(*keys).agg(
        F.first("cohort_rules_json").alias("cohort_rules_json"),
        F.count("*").alias("games_played"),
        F.sum("team_win").alias("wins"),
    ).withColumn("losses", F.col("games_played") - F.col("wins"))
      .withColumn("win_rate", F.col("wins") / F.col("games_played"))
      .withColumn("eligible_for_ranking", F.col("games_played") >= min_games()))


@dp.materialized_view(name="gold.model_team_performance", table_properties=GOLD)
def model_team_performance():
    # models is a set, so multiple seats for one model count only once.
    teams = spark.read.table("gold.team_episode_results").withColumn(
        "model_name", F.explode("models"))
    return team_summary(teams, ["cohort_id", "team", "model_name"])


@dp.materialized_view(name="gold.model_pair_performance", table_properties=GOLD)
def model_pair_performance():
    players = spark.read.table("_gold_player_results")
    pairs = (players.alias("a").join(players.alias("b"),
        (F.col("a.episode_id") == F.col("b.episode_id"))
        & (F.col("a.team") == F.col("b.team"))
        & (F.col("a.player_id") < F.col("b.player_id"))
    ).select(
        F.col("a.cohort_id").alias("cohort_id"),
        F.col("a.cohort_rules_json").alias("cohort_rules_json"),
        F.col("a.episode_id").alias("episode_id"),
        F.col("a.team").alias("team"),
        F.least("a.model_name", "b.model_name").alias("model_a"),
        F.greatest("a.model_name", "b.model_name").alias("model_b"),
        F.col("a.is_winner").cast("long").alias("team_win"),
    ).dropDuplicates(["cohort_id", "episode_id", "team", "model_a", "model_b"]))
    return team_summary(pairs, ["cohort_id", "team", "model_a", "model_b"])


@dp.materialized_view(name="gold.team_lineup_performance", table_properties=GOLD)
def team_lineup_performance():
    return team_summary(spark.read.table("gold.team_episode_results"),
                        ["cohort_id", "team", "lineup_id", "lineup_json"])
