# ~/.bashrc

# Exit if not running interactively.
[[ $- != *i* ]] && return

# -----------------------------------------------------------------------------
# History
# -----------------------------------------------------------------------------

HISTCONTROL=ignoreboth
HISTSIZE=1000
HISTFILESIZE=2000

shopt -s histappend
shopt -s checkwinsize

# Allow Ctrl+S / Ctrl+Q to be used by applications.
stty -ixon

# -----------------------------------------------------------------------------
# less
# -----------------------------------------------------------------------------

[[ -x /usr/bin/lesspipe ]] && eval "$(SHELL=/bin/sh lesspipe)"

# -----------------------------------------------------------------------------
# Prompt
# -----------------------------------------------------------------------------

if [[ -z ${debian_chroot:-} && -r /etc/debian_chroot ]]; then
    debian_chroot=$(< /etc/debian_chroot)
fi

case "$TERM" in
    xterm-color|*-256color)
        color_prompt=yes
        ;;
esac

if [[ -x /usr/bin/tput ]] && tput setaf 1 >/dev/null 2>&1; then
    color_prompt=yes
fi

# -----------------------------------------------------------------------------
# CLEAN PROMPT (UPDATED)
# -----------------------------------------------------------------------------
# Shows: ~/projects/dir 

if [[ $color_prompt == yes ]]; then
    PS1='${debian_chroot:+($debian_chroot)}\[\e[1;34m\]\w\[\e[0m\] > '
else
    PS1='${debian_chroot:+($debian_chroot)}\w > '
fi

case "$TERM" in
    xterm*|rxvt*)
        PS1="\[\e]0;${debian_chroot:+($debian_chroot)}\w\a\]$PS1"
        ;;
esac

unset color_prompt

# -----------------------------------------------------------------------------
# Colors & aliases
# -----------------------------------------------------------------------------

if [[ -x /usr/bin/dircolors ]]; then
    if [[ -r ~/.dircolors ]]; then
        eval "$(dircolors -b ~/.dircolors)"
    else
        eval "$(dircolors -b)"
    fi

    alias ls='ls --color=auto'
    alias grep='grep --color=auto'
    alias fgrep='fgrep --color=auto'
    alias egrep='egrep --color=auto'
fi

alias ll='ls -alF'
alias la='ls -A'
alias l='ls -CF'

# -----------------------------------------------------------------------------
# Claude
# -----------------------------------------------------------------------------

alias clauded='claude --dangerously-skip-permissions'

ask() {
  claude -p "$*"
}

# -----------------------------------------------------------------------------
# tmux
# -----------------------------------------------------------------------------

alias t='tmux new-session -A -s "$(basename "$PWD") $(printf "%s" "$PWD" | shasum -a 256 | cut -c1-4)"'
alias tl='tmux list-sessions -F "#{s/ [a-f0-9][a-f0-9][a-f0-9][a-f0-9]$//:session_name}" 2>/dev/null || echo "no sessions"'
alias ta='tmux attach-session'
alias to='tmux attach-session -t'

# -----------------------------------------------------------------------------
# User aliases
# -----------------------------------------------------------------------------

[[ -f ~/.bash_aliases ]] && source ~/.bash_aliases



