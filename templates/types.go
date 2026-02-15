package templates

type LogArgs struct {
	ID        string
	Topic     string
	Timestamp string
	Json      string
	JsonTree  map[string]any
}

type LogsPageArgs struct {
	ProfileIDs        []string
	Topics            []string
	InputTopicOptions []string
}
