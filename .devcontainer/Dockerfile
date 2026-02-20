FROM debian:bookworm-slim

ARG USERNAME=vscode
ARG USER_UID=501
ARG USER_GID=20

# Install only what you need
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      sudo \
      ca-certificates \
      git \
      curl \
 && rm -rf /var/lib/apt/lists/*

# Create group if needed
RUN if getent group ${USER_GID} >/dev/null; then \
      GROUP_NAME=$(getent group ${USER_GID} | cut -d: -f1); \
    else \
      GROUP_NAME=${USERNAME}; \
      groupadd --gid ${USER_GID} ${GROUP_NAME}; \
    fi

# Create user
RUN useradd -m -s /bin/bash \
      --uid ${USER_UID} \
      --gid ${USER_GID} \
      ${USERNAME} \
 && echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME} \
 && chmod 0440 /etc/sudoers.d/${USERNAME}

USER ${USERNAME}
WORKDIR /workspaces