"""Werewolf gold pipeline v1: model, role, and team performance.

DEPLOYMENT
Add this file to a Lakeflow Spark Declarative Pipeline. Set:
  werewolf.silver_schema = <catalog>.<schema>  (required; contains players/episodes)
  werewolf.gold_min_games = 20               (optional; descriptive sample filter)
Choose the gold destination catalog/schema in pipeline settings. Run the silver
pipeline successfully before this pipeline. All outputs are materialized views
with quality=gold; the existing bronze/silver pipeline is not modified.

OUTPUTS / GRAINS
  gold_model_role_performance: cohort/model/role/team
  gold_model_overall_performance: cohort/model
  gold_model_team_performance: cohort/team/model
  gold_model_pair_performance: cohort/team/unordered model pair
  gold_team_lineup_performance: cohort/team/exact model-and-role lineup
  gold_team_episode_results: episode/team (auditable team outcomes)

METRIC DEFINITIONS
Win rates are fractions in [0, 1]. Model/role results count player appearances;
overall also reports episode_equal_win_rate (average within-episode win share,
then average across episodes), so repeated seats do not overweight an episode.
role_balanced_win_rate equally weights OBSERVED roles, not missing roles. Compare
models with the same roles_observed for that metric. Survival is separate from
winning; surviving_win_rate uses silver.players.is_alive_winner.

Team metrics count an episode/team once. A model pair means two distinct players
on the SAME faction. Same-model pairs require two seats; duplicate model pairs
within a faction/game count once. Lineups retain model multiplicity and role
assignments but ignore seat order. Pair performance is association, not evidence
of causal synergy: opponents, teammates, and assignment frequencies still differ.

Only completed episodes with a known winner and terminated_with_agent_error=false
enter aggregates; unknown/null termination flags are excluded. Nonterminal action
errors remain included. Invalid keys,
missing episode joins, inconsistent team outcomes, or inconsistent winner/alive
flags fail validation. Cohorts separate versions, roster roles, and the listed
gameplay rules; expand RULE_FIELDS if future formats add relevant rules.

eligible_for_ranking requires distinct episode count >= gold_min_games. It is a
sample-size filter, not a statistical significance guarantee. Shared episodes
make observations correlated; no independence-based confidence intervals are
claimed. Overall rankings reflect the observed role/opponent mix.

EXAMPLE SQL (run in the gold target schema; choose ONE cohort for comparison)
-- Which models perform best at which roles?
SELECT * FROM gold_model_role_performance
WHERE eligible_for_ranking
ORDER BY cohort_id, role, team, win_rate DESC, games_played DESC;
-- Which models perform best overall?
SELECT * FROM gold_model_overall_performance
WHERE eligible_for_ranking
ORDER BY cohort_id, episode_equal_win_rate DESC, games_played DESC;
-- Which models perform best/worst together? Reverse DESC to ASC for worst.
SELECT * FROM gold_model_pair_performance
WHERE eligible_for_ranking
ORDER BY cohort_id, team, win_rate DESC, games_played DESC;
-- Whole-team composition comparison, with the same best/worst ordering.
SELECT * FROM gold_team_lineup_performance
WHERE eligible_for_ranking
ORDER BY cohort_id, team, win_rate DESC, games_played DESC;

VALIDATION AFTER DEPLOYMENT
For 74788868 alone: 2 team-episode rows, sizes 2 and 6, Werewolves win.
Model appearances sum to 8; player wins sum to 2; surviving wins sum to 2.
The two werewolves are GPT-5.4 mini and Claude Opus 4.6: their Werewolves
pair has one game and one win. With the default threshold, none is rank-eligible.
The sample has 16 same-faction player pairs but only 12 distinct faction/model
pairs, and 7 distinct faction/model appearances. These reference totals were
checked locally against the JSON; the Spark pipeline has not been executed here.
Across any corpus, rates must be in [0, 1], wins + losses = denominator,
and games_played <= player_appearances for player-level aggregates.

References:
https://docs.databricks.com/aws/en/ldp/transform
https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-materialized-view
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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


@dp.materialized_view(name="gold_model_role_performance", table_properties=GOLD)
def model_role_performance():
    return player_summary(["cohort_id", "model_name", "role", "team"])


@dp.materialized_view(name="gold_model_overall_performance", table_properties=GOLD)
def model_overall_performance():
    keys = ["cohort_id", "model_name"]
    per_episode = (spark.read.table("_gold_player_results")
                   .groupBy(*keys, "episode_id")
                   .agg(F.avg(F.col("is_winner").cast("double"))
                        .alias("episode_win_share"))
                   .groupBy(*keys).agg(F.avg("episode_win_share")
                                      .alias("episode_equal_win_rate")))
    roles = (spark.read.table("gold_model_role_performance")
             .groupBy(*keys).agg(
                 F.avg("win_rate").alias("role_balanced_win_rate"),
                 F.sort_array(F.collect_set("role")).alias("roles_observed"),
                 F.min("games_played").alias("min_games_in_observed_role")))
    return player_summary(keys).join(per_episode, keys).join(roles, keys)


@dp.materialized_view(name="gold_team_episode_results", table_properties=GOLD)
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


@dp.materialized_view(name="gold_model_team_performance", table_properties=GOLD)
def model_team_performance():
    # models is a set, so multiple seats for one model count only once.
    teams = spark.read.table("gold_team_episode_results").withColumn(
        "model_name", F.explode("models"))
    return team_summary(teams, ["cohort_id", "team", "model_name"])


@dp.materialized_view(name="gold_model_pair_performance", table_properties=GOLD)
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


@dp.materialized_view(name="gold_team_lineup_performance", table_properties=GOLD)
def team_lineup_performance():
    return team_summary(spark.read.table("gold_team_episode_results"),
                        ["cohort_id", "team", "lineup_id", "lineup_json"])
