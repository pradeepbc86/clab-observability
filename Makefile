.PHONY: help deploy destroy validate monitor logs clean

help:
	@echo "Targets:"
	@echo "  make deploy       - Deploy ContainerLab topology and observability stack"
	@echo "  make destroy      - Destroy lab and observability services"
	@echo "  make validate     - Validate BGP neighbors and connectivity"
	@echo "  make monitor      - Tail docker-compose logs for all services"
	@echo "  make logs         - Show all service logs"
	@echo "  make clean        - Clean up volumes and data"

deploy: .PHONY
	@echo "Starting ContainerLab topology..."
	clab deploy -t topology/lab.clab.yml
	@echo "Waiting for nodes to boot..."
	sleep 10
	@echo "Starting observability stack..."
	docker-compose up -d
	@echo "Deployed: Grafana (3000), ClickHouse (8123), Kibana (5601), Prometheus (9090)"

destroy: .PHONY
	@echo "Destroying ContainerLab topology..."
	clab destroy -t topology/lab.clab.yml --cleanup
	@echo "Stopping observability stack..."
	docker-compose down -v
	@echo "Done."

validate: .PHONY
	@echo "Validating BGP neighbors on spine1..."
	docker exec bgp-observability-spine1 vtysh -c "show bgp summary"
	@echo "Validating BGP neighbors on leaf1..."
	docker exec bgp-observability-leaf1 vtysh -c "show bgp summary"
	@echo "Validating BGP neighbors on leaf2..."
	docker exec bgp-observability-leaf2 vtysh -c "show bgp summary"
	@echo "Checking OpenBMP connectivity..."
	curl -s http://localhost:8123/ping || echo "ClickHouse not ready"

monitor: .PHONY
	docker-compose logs -f

logs: .PHONY
	docker-compose logs

clean: .PHONY
	@echo "Cleaning up..."
	docker-compose down -v
	rm -rf data/
	rm -rf temp/
	@echo "Cleaned."
