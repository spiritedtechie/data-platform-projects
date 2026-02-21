FROM debian:trixie-slim

ARG USERNAME=vscode
ARG USER_UID=501
ARG USER_GID=20

# Install base tooling + runtimes
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      sudo \
      ca-certificates \
      git \
      curl \
      make \
      python3.13 \
      python3.13-venv \
      python3-pip \
 && JAVA_PKG="" \
 && for p in \
      default-jre-headless \
      default-jre \
      openjdk-21-jre-headless \
      openjdk-21-jre \
      openjdk-17-jre-headless \
      openjdk-17-jre; do \
      if apt-cache show "$p" >/dev/null 2>&1; then \
        JAVA_PKG="$p"; \
        break; \
      fi; \
    done \
 && test -n "$JAVA_PKG" \
 && apt-get install -y --no-install-recommends "$JAVA_PKG" \
 && DOCKER_CLI_PKG="" \
 && for p in \
      docker.io \
      docker-cli \
      moby-cli; do \
      if apt-cache show "$p" >/dev/null 2>&1; then \
        DOCKER_CLI_PKG="$p"; \
        break; \
      fi; \
    done \
 && if [ -n "$DOCKER_CLI_PKG" ]; then \
      apt-get install -y --no-install-recommends "$DOCKER_CLI_PKG"; \
    fi \
 && ln -sf /usr/bin/python3.13 /usr/local/bin/python \
 && ln -sf /usr/bin/pip3 /usr/local/bin/pip \
 && mkdir -p /usr/lib/jvm \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/default-java \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install dbt CLI packages in a dedicated virtualenv
RUN python3.13 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/venv/bin/pip install --no-cache-dir \
      dbt-core==1.11.6 \
      dbt-spark[session]==1.10.1 \
      pytest \
      ruff \
      sqlfluff \
      sqlfluff-templater-dbt

ENV PATH="/opt/venv/bin:${JAVA_HOME}/bin:${PATH}"

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
