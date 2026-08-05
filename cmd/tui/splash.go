package main

import (
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// --- pixel-art block font, just the letters KAIROS needs ---

var blockFont = map[rune][]string{
	'K': {
		"█  █",
		"█ █ ",
		"██  ",
		"█ █ ",
		"█  █",
	},
	'A': {
		" ██ ",
		"█  █",
		"████",
		"█  █",
		"█  █",
	},
	'I': {
		"███",
		" █ ",
		" █ ",
		" █ ",
		"███",
	},
	'R': {
		"███ ",
		"█  █",
		"███ ",
		"█ █ ",
		"█  █",
	},
	'O': {
		" ██ ",
		"█  █",
		"█  █",
		"█  █",
		" ██ ",
	},
	'S': {
		" ███",
		"█   ",
		" ██ ",
		"   █",
		"███ ",
	},
}

func renderBlockWord(word string, revealed int, style lipgloss.Style) string {
	rows := make([]string, 5)
	count := 0
	for _, r := range word {
		glyph, ok := blockFont[r]
		if !ok {
			continue
		}
		show := count < revealed
		for i := 0; i < 5; i++ {
			if show {
				rows[i] += style.Render(glyph[i]) + "  "
			} else {
				rows[i] += strings.Repeat(" ", lipgloss.Width(glyph[i])) + "  "
			}
		}
		count++
	}
	return strings.Join(rows, "\n")
}

// --- splash state ---

const (
	splashTicks   = 14 // chronos ticks before kairos strikes
	splashWord    = "KAIROS"
	tickInterval  = 90 * time.Millisecond
	letterReveal  = 140 * time.Millisecond
	holdAfterWord = 900 * time.Millisecond
)

type splashPhase int

const (
	phaseChronos splashPhase = iota // even ticks crossing the screen, indifferent
	phaseStrike                     // the tick that lands "right" — a flash
	phaseReveal                     // KAIROS assembles, letter by letter
	phaseHold                       // sit on the finished title + tagline
	phaseDone                       // transition to dashboard
)

type splashModel struct {
	phase    splashPhase
	tickN    int
	revealed int
	width    int
}

type splashTickMsg time.Time

func splashTick(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg { return splashTickMsg(t) })
}

func newSplash() splashModel {
	return splashModel{phase: phaseChronos, width: 60}
}

func (s splashModel) Init() tea.Cmd {
	return splashTick(tickInterval)
}

// Update advances the splash animation. Returns done=true once it's
// finished (or the user skipped it), signaling the parent model to
// switch to the dashboard.
func (s splashModel) Update(msg tea.Msg) (splashModel, tea.Cmd, bool) {
	switch msg.(type) {
	case tea.KeyMsg:
		return s, nil, true // any key skips straight to the dashboard
	case splashTickMsg:
		switch s.phase {
		case phaseChronos:
			s.tickN++
			if s.tickN >= splashTicks {
				s.phase = phaseStrike
				return s, splashTick(180 * time.Millisecond), false
			}
			return s, splashTick(tickInterval), false

		case phaseStrike:
			s.phase = phaseReveal
			return s, splashTick(letterReveal), false

		case phaseReveal:
			s.revealed++
			if s.revealed >= len(splashWord) {
				s.phase = phaseHold
				return s, splashTick(holdAfterWord), false
			}
			return s, splashTick(letterReveal), false

		case phaseHold:
			s.phase = phaseDone
			return s, nil, true
		}
	}
	return s, nil, false
}

func (s splashModel) View() string {
	var chronosLine string
	{
		// An evenly-ticking dotted line — indifferent clock time — that
		// the cursor crosses left to right. On the strike, it flashes.
		track := make([]string, 40)
		for i := range track {
			track[i] = barTrack.Render("·")
		}
		pos := int(float64(s.tickN) / float64(splashTicks) * float64(len(track)-1))
		if pos < 0 {
			pos = 0
		}
		if pos > len(track)-1 {
			pos = len(track) - 1
		}
		switch s.phase {
		case phaseStrike:
			track[pos] = barHigh.Bold(true).Render("●")
		case phaseChronos:
			track[pos] = dimStyle.Render("○")
		default:
			track[pos] = barHigh.Render("●")
		}
		chronosLine = strings.Join(track, "")
	}

	label := dimStyle.Render("chronos — clock time, indifferent")
	if s.phase == phaseStrike {
		label = titleStyle.Render("kairos — the right moment, arrived")
	} else if s.phase >= phaseReveal {
		label = titleStyle.Render("kairos — the right moment, arrived")
	}

	var title string
	switch s.phase {
	case phaseChronos, phaseStrike:
		title = "" // not shown yet — chronos hasn't yielded the moment
	default:
		title = renderBlockWord(splashWord, s.revealed, barHigh)
	}

	tagline := ""
	if s.phase == phaseHold || (s.phase == phaseReveal && s.revealed == len(splashWord)) {
		tagline = dimStyle.Render("a distributed job queue that knows when to run")
	}

	parts := []string{"", chronosLine, label, ""}
	if title != "" {
		parts = append(parts, title, "")
	}
	if tagline != "" {
		parts = append(parts, tagline, "")
	}
	parts = append(parts, helpStyle.Render("press any key to skip"))

	return lipgloss.NewStyle().
		Padding(2, 4).
		Render(strings.Join(parts, "\n"))
}
