package gemini

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "gemini",
	Label:       "Gemini AI",
	Description: "Execute structured prompt queries against Google Gemini model configurations.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "gemini",
		"fields": {
			"apiKey":            { "name": "apiKey", "type": "string", "default": "", "required": true },
			"model":             { "name": "model", "type": "string", "default": "gemini-2.5-flash", "required": true },
			"prompt":            { "name": "prompt", "type": "string", "default": "", "required": true },
			"systemInstruction": { "name": "systemInstruction", "type": "string", "default": "", "required": false },
			"key":               { "name": "key", "type": "string", "default": "gemini_response", "required": true },
			"temperature":       { "name": "temperature", "type": "number", "default": 0.7, "required": false },
			"jsonMode":          { "name": "jsonMode", "type": "boolean", "default": false, "required": false }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	Run: run,
}

func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
	cfg := nCtx.Config
	apiKey, _ := cfg["apiKey"].(string)
	model, _ := cfg["model"].(string)
	if model == "" {
		model = "gemini-2.5-flash"
	}
	prompt, _ := cfg["prompt"].(string)
	systemInstruction, _ := cfg["systemInstruction"].(string)
	key, _ := cfg["key"].(string)
	if key == "" {
		key = "gemini_response"
	}
	temperature := 0.7
	if f, ok := nodekit.Number(cfg["temperature"]); ok {
		temperature = f
	}
	jsonMode := false
	if b, ok := cfg["jsonMode"].(bool); ok {
		jsonMode = b
	}

	if apiKey == "" {
		return nil, fmt.Errorf("Gemini node: API key is required")
	}
	if prompt == "" {
		return nil, fmt.Errorf("Gemini node: Prompt is required")
	}

	responseMimeType := "text/plain"
	if jsonMode {
		responseMimeType = "application/json"
	}

	payload := map[string]any{
		"contents": []any{
			map[string]any{"parts": []any{map[string]any{"text": prompt}}},
		},
		"generationConfig": map[string]any{
			"temperature":     temperature,
			"responseMimeType": responseMimeType,
		},
	}
	if systemInstruction != "" {
		payload["systemInstruction"] = map[string]any{
			"parts": []any{map[string]any{"text": systemInstruction}},
		}
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("Gemini node execution failed: %v", err)
	}

	url := "https://generativelanguage.googleapis.com/v1beta/models/" + model + ":generateContent?key=" + apiKey
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("Gemini node execution failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Gemini node execution failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Gemini node execution failed: %v", err)
	}

	if resp.StatusCode >= 400 {
		var errorJSON map[string]any
		serverMsg := http.StatusText(resp.StatusCode)
		if err := json.Unmarshal(respBody, &errorJSON); err == nil {
			if e, ok := errorJSON["error"].(map[string]any); ok {
				if m, ok := e["message"].(string); ok && m != "" {
					serverMsg = m
				}
			}
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, serverMsg)
	}

	var responseData map[string]any
	if err := json.Unmarshal(respBody, &responseData); err != nil {
		return nil, fmt.Errorf("Gemini node execution failed: %v", err)
	}

	candidate := firstCandidate(responseData)
	textOutput := candidateText(candidate)
	if textOutput == "" {
		return nil, fmt.Errorf(
			"Gemini API returned an empty response. Verify your prompt or safety limits.",
		)
	}

	data := any(textOutput)
	if jsonMode {
		var parsed any
		if err := json.Unmarshal([]byte(textOutput), &parsed); err == nil {
			data = parsed
		}
	}

	finishReason := ""
	if c, ok := candidate.(map[string]any); ok {
		if fr, ok := c["finishReason"].(string); ok {
			finishReason = fr
		}
	}
	usageMetadata := map[string]any{}
	if u, ok := responseData["usageMetadata"].(map[string]any); ok {
		usageMetadata = u
	}

	result := map[string]any{
		"data":          data,
		"model":         model,
		"finishReason":  finishReason,
		"usageMetadata": usageMetadata,
	}

	return nodekit.PatchMutator(map[string]any{key: result}), nil
}

func firstCandidate(responseData map[string]any) any {
	candidates, ok := responseData["candidates"].([]any)
	if !ok || len(candidates) == 0 {
		return nil
	}
	return candidates[0]
}

func candidateText(candidate any) string {
	c, ok := candidate.(map[string]any)
	if !ok {
		return ""
	}
	content, ok := c["content"].(map[string]any)
	if !ok {
		return ""
	}
	parts, ok := content["parts"].([]any)
	if !ok || len(parts) == 0 {
		return ""
	}
	if part, ok := parts[0].(map[string]any); ok {
		if text, ok := part["text"].(string); ok {
			return text
		}
	}
	return ""
}