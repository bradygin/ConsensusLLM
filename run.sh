#!/bin/bash

# Name of the tmux session
SESSION_NAME="distributed_system"

# Path to your virtual environment's activation script
VENV_PATH="./venv/bin/activate"

commands=(
  "source $VENV_PATH && python3 network_server.py"
  "source $VENV_PATH && python3 server.py 1"
  "source $VENV_PATH && python3 server.py 2"
  "source $VENV_PATH && python3 server.py 3"
)

# Create a new tmux session and run the first command (network server)
tmux new-session -d -s "$SESSION_NAME" "${commands[0]}"

# Split the window and run the remaining commands
for ((i=1; i<${#commands[@]}; i++)); do
  # For the first split (i=1), split horizontally
  if [ "$i" -eq 1 ]; then
    tmux split-window -h -t "$SESSION_NAME"
  else
    # For subsequent splits, split vertically
    tmux split-window -v -t "$SESSION_NAME"
  fi
  # Send the command to the new (current) pane
  tmux send-keys -t "$SESSION_NAME" "${commands[$i]}" C-m
done

# Arrange all panes in a tiled layout
tmux select-layout -t "$SESSION_NAME" tiled

# Attach to the tmux session
tmux attach-session -t "$SESSION_NAME"