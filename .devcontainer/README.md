To track network traffic in the devcontainer, use the `nicolaka/netshoot` image, which provides various network troubleshooting tools.

For example:
```
# List running containers to find the devcontainer name
docker ps

# Replace <your_devcontainer_name> with the actual name of the devcontainer
docker run --rm -it \
  --network container:<your_devcontainer_name> \
  --cap-add=NET_ADMIN \
  --cap-add=NET_RAW \
  nicolaka/netshoot

# Once inside the netshoot container, use tcpdump to monitor traffic on the eth0 interface
tcpdump -i eth0 -nnn
```