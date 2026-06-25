defmodule MiniWa.Agent.Runner do
  require Logger

  @api_url "http://localhost:11434/v1/chat/completions"
  @model "qwen2.5:7b"
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

  @doc """
  Run the agent with a user message and conversation history.
  History is a list of %{"role" => "user"|"assistant", "content" => string}.
  Returns {:ok, %{response, tool_calls, files}} or {:error, reason}.
  """
  def run(user_message, history) do
    messages = build_messages(history, user_message)
    loop(messages, [], [], 0)
  end

  # ─── Private ────────────────────────────────────────────────────────────────

  defp build_messages(history, user_message) do
    system_msg = %{"role" => "system", "content" => @system_prompt}

    history_msgs =
      Enum.map(history, fn msg ->
        %{"role" => msg["role"], "content" => to_string(msg["content"])}
      end)

    [system_msg] ++ history_msgs ++ [%{"role" => "user", "content" => user_message}]
  end

  defp loop(_messages, _files, _calls, turns) when turns >= @max_turns do
    {:error, "Agent exceeded maximum turns (#{@max_turns})"}
  end

  defp loop(messages, files_acc, calls_acc, turns) do
    case call_api(messages) do
      {:ok, choice} ->
        message = choice["message"] || %{}
        tool_calls = resolve_tool_calls(choice, message)

        if tool_calls != [] do
          {tool_result_msgs, new_files, new_calls} = execute_tools(tool_calls)

          assistant_msg = %{
            "role" => "assistant",
            "content" => message["content"],
            "tool_calls" => tool_calls
          }

          updated = messages ++ [assistant_msg] ++ tool_result_msgs
          loop(updated, files_acc ++ new_files, calls_acc ++ new_calls, turns + 1)
        else
          {:ok, %{response: message["content"] || "", files: files_acc, tool_calls: calls_acc}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # 1. Proper structured tool_calls (any finish_reason — some models say "stop" anyway)
  defp resolve_tool_calls(_choice, %{"tool_calls" => tcs}) when is_list(tcs) and tcs != [], do: tcs

  # 2. Model embedded a JSON tool call in plain text content — parse and synthesise
  defp resolve_tool_calls(_choice, %{"content" => content}) when is_binary(content) do
    case extract_text_tool_call(content) do
      nil -> []
      call -> [call]
    end
  end

  defp resolve_tool_calls(_choice, _message), do: []

  defp extract_text_tool_call(text) do
    # Strip markdown code fences, then find every `{` and try to parse balanced JSON from it
    clean = Regex.replace(~r/```[a-z]*\n?/, text, "")

    result =
      clean
      |> find_brace_positions()
      |> Enum.find_value(fn pos ->
        substr = String.slice(clean, pos..-1//1)
        json = extract_balanced_braces(substr)
        with json when is_binary(json) <- json,
             {:ok, parsed} <- Jason.decode(json),
             name when is_binary(name) <- parsed["name"],
             args when not is_nil(args) <- parsed["arguments"] || parsed["parameters"] || parsed["input"] do
          args = if is_binary(args), do: Jason.decode!(args), else: args
          id = "synthetic_#{:erlang.unique_integer([:positive])}"
          %{"id" => id, "type" => "function", "function" => %{"name" => name, "arguments" => Jason.encode!(args)}}
        else
          _ -> nil
        end
      end)

    result
  end

  defp find_brace_positions(text) do
    text
    |> String.graphemes()
    |> Enum.with_index()
    |> Enum.filter(fn {ch, _} -> ch == "{" end)
    |> Enum.map(fn {_, i} -> i end)
  end

  defp extract_balanced_braces(text) do
    text
    |> String.graphemes()
    |> Enum.reduce_while({0, [], false}, fn ch, {depth, acc, started} ->
      new_depth =
        case ch do
          "{" -> depth + 1
          "}" -> depth - 1
          _ -> depth
        end

      new_started = started || ch == "{"

      if new_started and new_depth == 0 do
        {:halt, {:done, [ch | acc]}}
      else
        {:cont, {new_depth, [ch | acc], new_started}}
      end
    end)
    |> then(fn
      {:done, chars} -> chars |> Enum.reverse() |> Enum.join()
      _ -> nil
    end)
  end

  defp call_api(messages) do
    body =
      Jason.encode!(%{
        "model" => @model,
        "messages" => messages,
        "tools" => MiniWa.Agent.Tools.definitions(),
        "stream" => false
      })

    headers = [{"content-type", "application/json"}]

    Logger.debug("[Agent] Calling Ollama (#{@model}), #{length(messages)} messages")

    case :hackney.request(:post, @api_url, headers, body, [:with_body, recv_timeout: 120_000]) do
      {:ok, 200, _headers, resp_body} ->
        response = Jason.decode!(resp_body)
        choice = get_in(response, ["choices", Access.at(0)])
        {:ok, choice}

      {:ok, status, _headers, resp_body} ->
        Logger.error("[Agent] Ollama error #{status}: #{resp_body}")
        {:error, "Ollama returned #{status}: #{resp_body}"}

      {:error, reason} ->
        Logger.error("[Agent] HTTP error: #{inspect(reason)}")
        {:error, "HTTP request failed: #{inspect(reason)}"}
    end
  end

  defp execute_tools(tool_calls) do
    Enum.reduce(tool_calls, {[], [], []}, fn tc, {result_msgs, files, calls} ->
      id = tc["id"]
      name = get_in(tc, ["function", "name"])
      input = tc |> get_in(["function", "arguments"]) |> decode_arguments()

      Logger.info("[Agent] Executing tool: #{name}")
      {output, new_files} = MiniWa.Agent.Tools.execute(name, input)

      result_msg = %{
        "role" => "tool",
        "tool_call_id" => id,
        "content" => output
      }

      {result_msgs ++ [result_msg], files ++ new_files, calls ++ [name]}
    end)
  end

  # Ollama may return arguments as a JSON string or already-decoded map
  defp decode_arguments(args) when is_binary(args), do: Jason.decode!(args)
  defp decode_arguments(args) when is_map(args), do: args
  defp decode_arguments(_), do: %{}
end
