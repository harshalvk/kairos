// Command tui is an interactive terminal dashboard for a running Kairos
// deployment: queue depth per priority, dead-lettered jobs, and quick
// requeue/purge actions — polls the admin HTTP API.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const apiBase = "http://localhost:8080"

var (
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#C9A227"))
	dimStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#8B93A7"))
	errStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#E05252"))
	tabActive  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#0B0E14")).Background(lipgloss.Color("#C9A227")).Padding(0, 2)
	tabIdle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#8B93A7")).Padding(0, 2)
	helpStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#5A6172")).MarginTop(1)
	barHigh    = lipgloss.NewStyle().Foreground(lipgloss.Color("#E0BE4A"))
	barDefault = lipgloss.NewStyle().Foreground(lipgloss.Color("#5EEAD4"))
	barLow     = lipgloss.NewStyle().Foreground(lipgloss.Color("#8B93A7"))
	barTrack   = lipgloss.NewStyle().Foreground(lipgloss.Color("#1E2432"))
	labelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#E8E6DF")).Width(9)
	countStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#E8E6DF")).Bold(true)
	emptyStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#5A6172")).Italic(true)
)

const barWidth = 30

type tickMsg time.Time

type depthResp map[string]int64

type deadLetterJob struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Attempts  int    `json:"attempts"`
	LastError string `json:"last_error"`
}

type model struct {
	showSplash bool
	splash     splashModel

	tab       int // 0 = overview, 1 = dead letter
	depths    depthResp
	dead      []deadLetterJob
	deadTable table.Model
	err       error
	status    string
}

func fetchJSON[T any](url string, out *T) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			log.Printf("close response body: %v", cerr)
		}
	}()
	return json.NewDecoder(resp.Body).Decode(out)
}

func poll() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m model) Init() tea.Cmd {
	if m.showSplash {
		return m.splash.Init()
	}
	return poll()
}

func refresh() (depthResp, []deadLetterJob, error) {
	var depths depthResp
	if err := fetchJSON(apiBase+"/queue/depth", &depths); err != nil {
		return nil, nil, err
	}
	var dead []deadLetterJob
	if err := fetchJSON(apiBase+"/jobs/dead-letter", &dead); err != nil {
		// Queue depth succeeded but dead-letter fetch failed — don't
		// treat the whole refresh as an error, just show what we have.
		dead = nil
	}
	return depths, dead, nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if m.showSplash {
		if k, ok := msg.(tea.KeyMsg); ok && (k.String() == "q" || k.String() == "ctrl+c") {
			return m, tea.Quit
		}
		var cmd tea.Cmd
		var done bool
		m.splash, cmd, done = m.splash.Update(msg)
		if done {
			m.showSplash = false
			return m, poll()
		}
		return m, cmd
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab":
			m.tab = (m.tab + 1) % 2
		case "r":
			if m.tab == 1 {
				row := m.deadTable.SelectedRow()
				if len(row) > 0 {
					id := row[0]
					go func() {
						req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, apiBase+"/jobs/dead-letter/"+id+"/requeue", nil)
						if err != nil {
							return
						}
						resp, err := http.DefaultClient.Do(req)
						if err != nil {
							log.Printf("requeue %s failed: %v", id, err)
							return
						}
						defer func() {
							if cerr := resp.Body.Close(); cerr != nil {
								log.Printf("close requeue response body: %v", cerr)
							}
						}()
					}()
					m.status = "requeued " + id
				}
			}
		}
	case tickMsg:
		depths, dead, err := refresh()
		if err != nil {
			m.err = err
		} else {
			m.err = nil
			m.depths = depths
			m.dead = dead
			rows := make([]table.Row, len(dead))
			for i, j := range dead {
				rows[i] = table.Row{j.ID, j.Type, fmt.Sprint(j.Attempts), truncate(j.LastError, 40)}
			}
			m.deadTable.SetRows(rows)
		}
		return m, poll()
	}
	var cmd tea.Cmd
	m.deadTable, cmd = m.deadTable.Update(msg)
	return m, cmd
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// renderBar draws a proportional bar for count against the largest
// value across all three priorities, so the relative shape of the
// backlog is visible at a glance, not just the raw numbers.
func renderBar(count, maxCount int64, style lipgloss.Style) string {
	filled := 0
	if maxCount > 0 {
		filled = int(float64(count) / float64(maxCount) * barWidth)
	}
	if count > 0 && filled == 0 {
		filled = 1 // always show at least a sliver for a nonzero count
	}
	if filled > barWidth {
		filled = barWidth
	}
	bar := style.Render(strings.Repeat("█", filled)) + barTrack.Render(strings.Repeat("░", barWidth-filled))
	return bar
}

func (m model) renderOverview() string {
	high, def, low := m.depths["high"], m.depths["default"], m.depths["low"]
	peak := high
	if def > peak {
		peak = def
	}
	if low > peak {
		peak = low
	}

	rows := []struct {
		label string
		count int64
		style lipgloss.Style
	}{
		{"high", high, barHigh},
		{"default", def, barDefault},
		{"low", low, barLow},
	}

	var b strings.Builder
	fmt.Fprintf(&b, "  %s\n", titleStyle.Render("Pending queue depth"))
	total := high + def + low
	for _, r := range rows {
		fmt.Printf(
			"%s %s %s\n",
			labelStyle.Render(r.label),
			renderBar(r.count, peak, r.style),
			countStyle.Render(fmt.Sprint(r.count)),
		)
	}
	b.WriteString("\n")
	b.WriteString(dimStyle.Render(fmt.Sprintf("total pending: %d", total)))
	return b.String()
}

func (m model) renderDeadLetter() string {
	if len(m.dead) == 0 {
		return titleStyle.Render("Dead letter") + "\n\n" + emptyStyle.Render("  nothing here — every job is behaving.")
	}
	return titleStyle.Render(fmt.Sprintf("Dead letter (%d)", len(m.dead))) + "\n\n" + m.deadTable.View()
}

func (m model) View() string {
	if m.showSplash {
		return m.splash.View()
	}

	tabs := lipgloss.JoinHorizontal(lipgloss.Top,
		styleTab(m.tab == 0, "Overview"),
		styleTab(m.tab == 1, fmt.Sprintf("Dead letter (%d)", len(m.dead))),
	)

	var body string
	if m.err != nil {
		body = errStyle.Render(fmt.Sprintf("error reaching admin api: %v", m.err))
	} else if m.tab == 0 {
		body = m.renderOverview()
	} else {
		body = m.renderDeadLetter()
	}

	help := helpStyle.Render("tab: switch panel · r: requeue selected · q: quit")
	status := ""
	if m.status != "" {
		status = "\n" + dimStyle.Render(m.status)
	}

	return "\n" + tabs + "\n\n" + body + status + "\n" + help + "\n"
}

func styleTab(active bool, label string) string {
	if active {
		return tabActive.Render(label)
	}
	return tabIdle.Render(label)
}

func main() {
	t := table.New(
		table.WithColumns([]table.Column{
			{Title: "ID", Width: 36},
			{Title: "Type", Width: 16},
			{Title: "Attempts", Width: 9},
			{Title: "Last error", Width: 40},
		}),
		table.WithFocused(true),
		table.WithHeight(12),
	)

	m := model{showSplash: true, splash: newSplash(), tab: 0, deadTable: t}
	if _, err := tea.NewProgram(m, tea.WithAltScreen()).Run(); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}
