defmodule MiniWa.Agent.AnthropicRunner do
  require Logger

  @api_url "https://api.anthropic.com/v1/messages"
  @model "claude-sonnet-4-6"
  @max_turns 10

  @system_prompt """
  You are a data agent for miniWA. You MUST call tools to answer every question. Never describe queries or show SQL/JSON — just call the tool.

  Rules:
  - Call query_db immediately for any data question. Do not explain first.
  - Call create_html_file for any chart or dashboard request.
  - Run multiple queries if needed to get a complete answer.

  Response style:
  - Write like a knowledgeable colleague, not a report. Short, direct sentences.
  - For simple facts: one or two sentences max. Do not pad with breakdowns the user didn't ask for.
  - For lists: use plain prose ("Sachin has 16, Veena has 11, Bhoomika has 11") not bullet points or headers unless there are more than 5 items.
  - Never use LaTeX or math notation. Write calculations in plain text or skip them entirely.
  - Never end with "Would you like..." or "Let me know if..." — just answer and stop.
  - No bold headers. No code blocks in answers.

  ScyllaDB tables (keyspace mini_wa):
  - messages: PK(conversation_id, message_id), cols: sender_id, recipient_id, content, sent_at_ms, media_type
  - users: PK(user_id), cols: registered_at
  - groups: PK(group_id), cols: name, created_by, created_at
  - group_members: PK(group_id, user_id), cols: added_by, share_from
  - user_groups: PK(user_id, group_id), cols: group_name
  - undelivered_messages: PK(recipient_id, message_id), cols: conversation_id, sender_id, content, type
  - analytics_counters: PK(name), cols: value — names: total, text_c, image_c, audio_c, video_c, lat_sum, lat_cnt, session_starts, session_crashes
  - analytics_hourly: PK(day YYYY-MM-DD), clustering(hour 0-23), cols: total, text_c, image_c, audio_c, video_c, lat_sum, lat_cnt
  - analytics_daily: PK(month YYYY-MM), clustering(day YYYY-MM-DD), same cols

  Notes: conversation_id for 1:1 = sorted join e.g. "alice:bob"; for groups = UUID. Use ALLOW FILTERING when filtering non-PK columns. No semicolons in queries.

  Counting rules (important — queries have an automatic LIMIT 100 applied):
  - NEVER count by fetching rows and summing. Always use COUNT(*) for any counting question.
  - To count messages per sender in a conversation: SELECT sender_id, COUNT(*) FROM mini_wa.messages WHERE conversation_id = '...' GROUP BY sender_id
  - To count total messages in a conversation: SELECT COUNT(*) FROM mini_wa.messages WHERE conversation_id = '...'
  - GROUP BY is supported on clustering key columns. COUNT(*) returns exact results and is not affected by LIMIT.

  HTML visual style: bg #0f1117, card #1a1f2e, text #e2e8f0, border #2d3748, green #25D366, blue #4299e1, orange #ed8936, teal #38b2ac, yellow #ecc94b. Use Plotly.js (https://cdn.jsdelivr.net/npm/plotly.js@2.35.2/dist/plotly.min.js) for all charts — set paper_bgcolor and plot_bgcolor to '#0f1117', font color to '#e2e8f0', gridcolor to '#2d3748'. Prefer interactive types: bar, scatter, line, pie, sunburst, treemap, heatmap, sankey, box, histogram. Add titles, axis labels, hover templates. Embed all data inline.
  """

  def run(user_message, history, model \\ @model) do
    api_key = System.get_env("ANTHROPIC_API_KEY")

    unless api_key do
      {:error, "ANTHROPIC_API_KEY environment variable is not set"}
    else
      messages = build_messages(history, user_message)
      loop(messages, [], [], api_key, model, 0)
    end
  end

  # ─── Private ────────────────────────────────────────────────────────────────

  defp build_messages(history, user_message) do
    base =
      Enum.map(history, fn msg ->
        %{"role" => msg["role"], "content" => to_string(msg["content"])}
      end)

    base ++ [%{"role" => "user", "content" => user_message}]
  end

  defp loop(_messages, _files, _calls, _key, _model, turns) when turns >= @max_turns do
    {:error, "Agent exceeded maximum turns (#{@max_turns})"}
  end

  defp loop(messages, files_acc, calls_acc, api_key, model, turns) do
    case call_api(messages, api_key, model) do
      {:ok, %{"stop_reason" => "tool_use", "content" => content}} ->
        {tool_result_blocks, new_files, new_calls} = execute_tools(content)

        updated_messages =
          messages ++
            [
              %{"role" => "assistant", "content" => content},
              %{"role" => "user", "content" => tool_result_blocks}
            ]

        loop(updated_messages, files_acc ++ new_files, calls_acc ++ new_calls, api_key, model, turns + 1)

      {:ok, response} ->
        content = Map.get(response, "content", [])
        text = extract_text(content)
        {:ok, %{response: text, files: files_acc, tool_calls: calls_acc}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp call_api(messages, api_key, model) do
    tools = MiniWa.Agent.Tools.anthropic_definitions()

    tools_with_cache =
      List.update_at(tools, -1, fn last ->
        Map.put(last, "cache_control", %{"type" => "ephemeral"})
      end)

    body =
      Jason.encode!(%{
        "model" => model,
        "max_tokens" => 8192,
        "system" => [
          %{
            "type" => "text",
            "text" => @system_prompt,
            "cache_control" => %{"type" => "ephemeral"}
          }
        ],
        "tools" => tools_with_cache,
        "messages" => messages
      })

    headers = [
      {"x-api-key", api_key},
      {"anthropic-version", "2023-06-01"},
      {"anthropic-beta", "prompt-caching-2024-07-31"},
      {"content-type", "application/json"}
    ]

    Logger.debug("[Agent/Claude] Calling #{model}, #{length(messages)} messages")

    case :hackney.request(:post, @api_url, headers, body, [:with_body, recv_timeout: 120_000]) do
      {:ok, 200, _headers, resp_body} ->
        {:ok, Jason.decode!(resp_body)}

      {:ok, status, _headers, resp_body} ->
        Logger.error("[Agent/Claude] API error #{status}: #{resp_body}")
        {:error, "Claude API returned #{status}"}

      {:error, reason} ->
        Logger.error("[Agent/Claude] HTTP error: #{inspect(reason)}")
        {:error, "HTTP request failed: #{inspect(reason)}"}
    end
  end

  defp execute_tools(content_blocks) do
    tool_blocks = Enum.filter(content_blocks, &(&1["type"] == "tool_use"))

    Enum.reduce(tool_blocks, {[], [], []}, fn block, {result_blocks, files, calls} ->
      name = block["name"]
      input = block["input"] || %{}
      id = block["id"]

      Logger.info("[Agent/Claude] Executing tool: #{name}")
      {output, new_files} = MiniWa.Agent.Tools.execute(name, input)

      result_block = %{
        "type" => "tool_result",
        "tool_use_id" => id,
        "content" => output
      }

      {result_blocks ++ [result_block], files ++ new_files, calls ++ [name]}
    end)
  end

  defp extract_text(content_blocks) when is_list(content_blocks) do
    content_blocks
    |> Enum.filter(&(&1["type"] == "text"))
    |> Enum.map_join("\n", & &1["text"])
  end

  defp extract_text(_), do: ""
end
