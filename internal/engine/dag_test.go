package engine

import "testing"

func TestValidateWorkflow(t *testing.T) {
	tests := []struct {
		name    string
		def     WorkflowDefinition
		wantErr bool
	}{
		{
			name: "valid dag",
			def: WorkflowDefinition{ID: "wf", Version: 1, Steps: []StepDefinition{
				{ID: "a", Type: "test"},
				{ID: "b", Type: "test", DependsOn: []string{"a"}},
				{ID: "c", Type: "test", DependsOn: []string{"a"}},
				{ID: "d", Type: "test", DependsOn: []string{"b", "c"}},
			}},
		},
		{
			name: "duplicate id",
			def: WorkflowDefinition{ID: "wf", Version: 1, Steps: []StepDefinition{
				{ID: "a", Type: "test"}, {ID: "a", Type: "test"},
			}},
			wantErr: true,
		},
		{
			name: "unknown dependency",
			def: WorkflowDefinition{ID: "wf", Version: 1, Steps: []StepDefinition{
				{ID: "a", Type: "test", DependsOn: []string{"missing"}},
			}},
			wantErr: true,
		},
		{
			name: "cycle",
			def: WorkflowDefinition{ID: "wf", Version: 1, Steps: []StepDefinition{
				{ID: "a", Type: "test", DependsOn: []string{"c"}},
				{ID: "b", Type: "test", DependsOn: []string{"a"}},
				{ID: "c", Type: "test", DependsOn: []string{"b"}},
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateWorkflow(tt.def)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateWorkflow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRunnableSteps(t *testing.T) {
	def := WorkflowDefinition{ID: "wf", Version: 1, Steps: []StepDefinition{
		{ID: "a", Type: "test"},
		{ID: "b", Type: "test", DependsOn: []string{"a"}},
		{ID: "c", Type: "test", DependsOn: []string{"a"}},
		{ID: "d", Type: "test", DependsOn: []string{"b", "c"}},
	}}

	statuses := map[string]StepStatus{
		"a": StepCompleted,
		"b": StepPending,
		"c": StepPending,
		"d": StepPending,
	}

	got := RunnableSteps(def, statuses)
	if len(got) != 2 || got[0].ID != "b" || got[1].ID != "c" {
		t.Fatalf("RunnableSteps() = %#v, want b then c", got)
	}
}
