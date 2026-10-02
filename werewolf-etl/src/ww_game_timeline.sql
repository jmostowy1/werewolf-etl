-- Materialized with the other ETL outputs; refreshed after its silver dependencies.
-- Source coordinates, rather than action-type priorities, define chronology.
CREATE OR REFRESH MATERIALIZED VIEW gold.game_timeline
COMMENT 'Werewolf action and outcome markers; includes invisible roster anchors for the game timeline.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH event_fields AS (
  SELECT e.*,
    named_struct('batch', batch_index, 'entry', entry_index) AS event_position,
    get_json_object(data_json, '$.actor_id') AS event_actor_id,
    get_json_object(data_json, '$.target_id') AS event_target_id,
    get_json_object(data_json, '$.message') AS event_message,
    CASE WHEN event_name = 'discussion_order' THEN 'discussion'
         WHEN event_name = 'vote_order' AND phase = 'Night' THEN 'wolf_vote'
         WHEN event_name = 'vote_order' THEN 'village_vote' END AS order_category,
    coalesce(
      from_json(get_json_object(data_json, '$.chat_order_of_player_ids'), 'ARRAY<STRING>'),
      from_json(get_json_object(data_json, '$.vote_order_of_player_ids'), 'ARRAY<STRING>')
    ) AS player_order
  FROM silver.events e
), actions AS (
  SELECT a.*,
    concat('action:', episode_id, ':', step_index, ':', seat_index) AS action_id,
    CASE WHEN action_type = 'ChatAction' THEN 'discussion'
         WHEN action_type = 'EliminateProposalAction' OR
              (action_type = 'VoteAction' AND phase = 'Night') THEN 'wolf_vote'
         WHEN action_type = 'VoteAction' THEN 'village_vote'
         WHEN action_type = 'HealAction' THEN 'heal'
         WHEN action_type = 'InspectAction' THEN 'inspect'
         ELSE action_type END AS action_category,
    CASE action_type WHEN 'ChatAction' THEN 'discussion'
         WHEN 'EliminateProposalAction' THEN 'eliminate_proposal_action'
         WHEN 'VoteAction' THEN 'vote_action'
         WHEN 'HealAction' THEN 'heal_action'
         WHEN 'InspectAction' THEN 'inspect_action'
         WHEN 'NoOpAction' THEN 'no_op_action' END AS telemetry_name
  FROM silver.actions a
), matched_actions AS (
  -- One timestamp per action, even when public/private telemetry is repeated.
  SELECT a.*, e.batch_index, e.entry_index, e.created_at, e.detailed_phase,
    coalesce(e.event_position, named_struct('batch', a.step_index, 'entry', CAST(-1 AS BIGINT))) AS action_position,
    CASE WHEN e.batch_index IS NULL THEN 'No matching action event'
         ELSE 'Matched action event' END AS timestamp_status
  FROM actions a LEFT JOIN event_fields e
    ON a.episode_id = e.episode_id AND a.step_index = e.batch_index
   AND a.telemetry_name = e.event_name AND a.actor_id = e.event_actor_id
   AND a.target_id <=> e.event_target_id
   AND (a.action_type <> 'ChatAction' OR a.message <=> e.event_message)
  QUALIFY row_number() OVER (
    PARTITION BY a.episode_id, a.step_index, a.seat_index
    ORDER BY CASE WHEN e.data_type = 'dict' THEN 0 ELSE 1 END, e.entry_index
  ) = 1
), order_events AS (
  SELECT episode_id, day, phase, order_category, event_position, player_order
  FROM event_fields
  WHERE order_category IS NOT NULL AND size(player_order) > 0
  QUALIFY row_number() OVER (
    PARTITION BY episode_id, day, phase, order_category, batch_index, to_json(player_order)
    ORDER BY entry_index
  ) = 1
), order_intervals AS (
  SELECT *, lead(event_position) OVER (
    PARTITION BY episode_id, day, phase, order_category ORDER BY event_position
  ) AS next_position FROM order_events
), order_slots AS (
  -- First occurrence preserves the protocol order, including rotated speakers.
  SELECT episode_id, day, phase, order_category, event_position, player_id,
    min(slot) AS protocol_slot
  FROM order_events LATERAL VIEW posexplode(player_order) p AS slot, player_id
  GROUP BY episode_id, day, phase, order_category, event_position, player_id
), scoped_actions AS (
  SELECT a.*, o.event_position AS session_position,
    coalesce(concat(o.event_position.batch, ':', o.event_position.entry), 'unrecorded') AS session_key,
    s.protocol_slot
  FROM matched_actions a LEFT JOIN order_intervals o
    ON a.episode_id = o.episode_id AND a.day <=> o.day AND a.phase <=> o.phase
   AND a.action_category = o.order_category AND a.action_position > o.event_position
   AND (o.next_position IS NULL OR a.action_position < o.next_position)
  LEFT JOIN order_slots s
    ON o.episode_id = s.episode_id AND o.event_position = s.event_position
   AND a.actor_id = s.player_id AND o.order_category = s.order_category
), preceding_actions AS (
  SELECT *, lag(protocol_slot) OVER (
    PARTITION BY episode_id, day, phase, action_category, session_key
    ORDER BY action_position, seat_index
  ) AS previous_protocol_slot
  FROM scoped_actions
), round_actions AS (
  -- Shared round advances when the recorded protocol order wraps. Never count
  -- occurrences separately for each actor: a skipped actor would misalign rounds.
  -- Without an order record, keep actions separate rather than invent a round.
  SELECT *, CASE WHEN protocol_slot IS NOT NULL THEN
    sum(CASE WHEN protocol_slot <= previous_protocol_slot THEN 1 ELSE 0 END) OVER (
      PARTITION BY episode_id, day, phase, action_category, session_key
      ORDER BY action_position, seat_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) END AS action_round
  FROM preceding_actions
), action_markers AS (
  SELECT episode_id, action_id AS marker_id, 'action' AS marker_kind, action_id,
    concat_ws(':', 'group', CAST(episode_id AS STRING), coalesce(CAST(day AS STRING), '?'),
      coalesce(phase, '?'), action_category, session_key,
      CASE WHEN protocol_slot IS NOT NULL THEN CAST(action_round AS STRING)
           ELSE concat('step-', step_index) END) AS group_id,
    actor_id AS display_player_id, actor_id, target_id, action_type, action_category,
    day, phase, detailed_phase, action_round, step_index, batch_index, entry_index,
    created_at, action_position AS source_position, 0 AS position_suffix,
    CAST(NULL AS STRING) AS outcome_type, CAST(NULL AS STRING) AS outcome_description,
    CASE WHEN protocol_slot IS NOT NULL THEN 'Recorded order; round inferred from order wrap'
         ELSE 'No protocol order; individual step' END AS grouping_status,
    timestamp_status,
    CASE WHEN action_category IN ('wolf_vote', 'village_vote') THEN true ELSE false END AS is_vote,
    CASE action_category WHEN 'wolf_vote' THEN 'Wolf vote' WHEN 'village_vote' THEN 'Village vote'
         WHEN 'discussion' THEN 'Discussion' WHEN 'heal' THEN 'Heal'
         WHEN 'inspect' THEN 'Inspect' ELSE action_type END AS action_label
  FROM round_actions
), outcome_candidates AS (
  SELECT e.*,
    CASE WHEN event_name = 'vote_result' THEN get_json_object(data_json, '$.elected_target_player_id')
         WHEN data_type = 'DayExileElectedDataEntry' THEN get_json_object(data_json, '$.elected_player_id')
         WHEN event_name = 'elimination' THEN get_json_object(data_json, '$.eliminated_player_id')
         WHEN event_name = 'heal_result' THEN get_json_object(data_json, '$.saved_player_id') END AS outcome_target,
    CASE WHEN phase = 'Night' THEN 'wolf_vote' ELSE 'village_vote' END AS vote_category
  FROM event_fields e
  WHERE event_name IN ('vote_result', 'elimination', 'heal_result')
), distinct_outcomes AS (
  SELECT * FROM outcome_candidates
  QUALIFY row_number() OVER (
    PARTITION BY episode_id, batch_index, day, phase, event_name, outcome_target, data_json
    ORDER BY CASE WHEN public THEN 0 ELSE 1 END, entry_index
  ) = 1
), outcome_kinds AS (
  -- A day exile records both election and elimination. A wolf election alone
  -- never implies death; a separate elimination or save event determines that.
  SELECT e.*, k.marker_kind,
    CASE k.marker_kind WHEN 'vote_result' THEN 1 WHEN 'save' THEN 2 ELSE 3 END AS position_suffix
  FROM distinct_outcomes e LATERAL VIEW explode(
    CASE WHEN data_type = 'DayExileElectedDataEntry' THEN array('vote_result', 'elimination')
         WHEN event_name = 'vote_result' THEN array('vote_result')
         WHEN event_name = 'heal_result' THEN array('save') ELSE array('elimination') END
  ) k AS marker_kind
), outcome_markers AS (
  SELECT e.episode_id,
    concat('event:', e.episode_id, ':', e.batch_index, ':', e.entry_index, ':', e.marker_kind) AS marker_id,
    e.marker_kind, CAST(NULL AS STRING) AS action_id,
    concat('result:', e.episode_id, ':', e.batch_index, ':', e.entry_index, ':', e.marker_kind) AS group_id,
    CASE WHEN e.outcome_target IS NULL OR e.outcome_target = '-1' THEN '__game__'
         ELSE e.outcome_target END AS display_player_id,
    CAST(NULL AS STRING) AS actor_id, e.outcome_target AS target_id,
    e.event_name AS action_type, e.vote_category AS action_category,
    e.day, e.phase, e.detailed_phase, CAST(NULL AS BIGINT) AS action_round,
    e.batch_index AS step_index, e.batch_index, e.entry_index, e.created_at,
    e.event_position AS source_position, e.position_suffix,
    CASE WHEN e.outcome_target = '-1' THEN 'No target selected'
         WHEN e.outcome_target IS NULL THEN 'Outcome has no structured player target'
         WHEN e.marker_kind = 'vote_result' THEN 'Elected'
         WHEN e.marker_kind = 'save' THEN 'Saved' ELSE 'Eliminated' END AS outcome_type,
    e.description AS outcome_description,
    CASE WHEN o.event_position IS NULL THEN 'Outcome event; no matching vote order'
         ELSE concat('Outcome for vote order ', o.event_position.batch, ':', o.event_position.entry) END AS grouping_status,
    'Recorded outcome event' AS timestamp_status, false AS is_vote,
    CASE e.marker_kind WHEN 'vote_result' THEN 'Elected target'
         WHEN 'save' THEN 'Saved' ELSE 'Eliminated' END AS action_label
  FROM outcome_kinds e LEFT JOIN order_intervals o
    ON e.episode_id = o.episode_id AND e.day <=> o.day AND e.phase <=> o.phase
   AND e.vote_category = o.order_category AND e.event_position > o.event_position
   AND (o.next_position IS NULL OR e.event_position < o.next_position)
), markers AS (
  SELECT * FROM action_markers UNION ALL SELECT * FROM outcome_markers
), group_positions AS (
  SELECT episode_id, group_id, min(named_struct('position', source_position, 'suffix', position_suffix)) AS first_position
  FROM markers GROUP BY episode_id, group_id
), ordered_groups AS (
  SELECT *, row_number() OVER (PARTITION BY episode_id ORDER BY first_position, group_id) - 1 AS group_order
  FROM group_positions
), located_markers AS (
  SELECT m.*, g.group_order,
    concat(lpad(CAST(g.group_order + 1 AS STRING), 3, '0'), ' · ', coalesce(m.phase, 'Unknown'), ' ',
      coalesce(CAST(m.day AS STRING), '?'), ' · ', m.action_label,
      CASE WHEN m.action_round IS NOT NULL THEN concat(' ', m.action_round + 1) ELSE '' END) AS group_label,
    concat(coalesce(m.phase, 'Unknown'), ' ', coalesce(CAST(m.day AS STRING), '?')) AS phase_label
  FROM markers m JOIN ordered_groups g USING (episode_id, group_id)
), enriched AS (
  SELECT m.episode_id, m.marker_id, m.marker_kind, m.action_id,
    m.group_id, m.group_order, m.group_label, m.phase_label,
    m.display_player_id, coalesce(p.seat_index, CAST(999 AS BIGINT)) AS display_seat_index,
    CASE WHEN m.display_player_id = '__game__' THEN 'Game outcome'
         ELSE concat(m.display_player_id, ' · ', coalesce(p.model_name, 'Unknown model')) END AS player_label,
    m.actor_id, p.model_name, p.role, p.team,
    m.target_id, CASE WHEN m.target_id = '-1' AND m.marker_kind = 'action' THEN 'Abstain'
                      WHEN m.target_id = '-1' THEN 'No target selected'
                      ELSE m.target_id END AS target_label,
    m.action_type, m.action_category, m.action_label, m.is_vote,
    m.day, m.phase, m.detailed_phase, m.action_round, m.step_index,
    m.batch_index, m.entry_index, m.created_at,
    m.outcome_type, m.outcome_description, m.grouping_status, m.timestamp_status,
    a.reasoning, a.message, a.perceived_threat_level,
    a.prompt_tokens, a.completion_tokens, a.reported_cost, a.error
  FROM located_markers m
  LEFT JOIN silver.players p ON m.episode_id = p.episode_id AND m.display_player_id = p.player_id
  LEFT JOIN silver.actions a ON m.marker_kind = 'action' AND m.episode_id = a.episode_id
    AND m.step_index = a.step_index AND m.actor_id = a.actor_id
), phase_anchors AS (
  SELECT * FROM enriched
  QUALIFY row_number() OVER (PARTITION BY episode_id, phase_label ORDER BY group_order, marker_id) = 1
), roster_anchors AS (
  -- Invisible marks retain every roster player even after elimination, including
  -- when the user narrows the chart to one day/phase. Text is never copied here.
  SELECT e.episode_id, concat('roster:', e.episode_id, ':', e.phase_label, ':', p.player_id) AS marker_id,
    'roster' AS marker_kind, CAST(NULL AS STRING) AS action_id,
    e.group_id, e.group_order, e.group_label, e.phase_label,
    p.player_id AS display_player_id, p.seat_index AS display_seat_index,
    concat(p.player_id, ' · ', p.model_name) AS player_label,
    CAST(NULL AS STRING) AS actor_id, p.model_name, p.role, p.team,
    CAST(NULL AS STRING) AS target_id, CAST(NULL AS STRING) AS target_label,
    CAST(NULL AS STRING) AS action_type, CAST(NULL AS STRING) AS action_category,
    'Roster' AS action_label, false AS is_vote,
    e.day, e.phase, CAST(NULL AS STRING) AS detailed_phase, CAST(NULL AS BIGINT) AS action_round,
    CAST(NULL AS BIGINT) AS step_index, CAST(NULL AS BIGINT) AS batch_index,
    CAST(NULL AS BIGINT) AS entry_index, CAST(NULL AS TIMESTAMP) AS created_at,
    CAST(NULL AS STRING) AS outcome_type, CAST(NULL AS STRING) AS outcome_description,
    'Roster anchor' AS grouping_status, CAST(NULL AS STRING) AS timestamp_status,
    CAST(NULL AS STRING) AS reasoning, CAST(NULL AS STRING) AS message,
    CAST(NULL AS STRING) AS perceived_threat_level, CAST(NULL AS BIGINT) AS prompt_tokens,
    CAST(NULL AS BIGINT) AS completion_tokens, CAST(NULL AS DOUBLE) AS reported_cost,
    CAST(NULL AS STRING) AS error
  FROM phase_anchors e JOIN silver.players p ON e.episode_id = p.episode_id
)
SELECT * FROM enriched
UNION ALL
SELECT * FROM roster_anchors;
