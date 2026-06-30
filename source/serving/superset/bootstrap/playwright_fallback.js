async page => {
  const registry = __SUPERSET_BOOTSTRAP_REGISTRY_JSON__;
  const baseUrl = __SUPERSET_BASE_URL__;
  const username = __SUPERSET_ADMIN_USERNAME__;
  const password = __SUPERSET_ADMIN_PASSWORD__;

  await page.goto(`${baseUrl}/login/`);
  await page.getByLabel("Username:").fill(username);
  await page.getByLabel("Password:").fill(password);
  await page.getByRole("button", { name: "Sign In" }).click();
  await page.waitForURL(/superset\/welcome/);

  await page.evaluate(
    async ({ registry }) => {
      const headers = {
        "Content-Type": "application/json",
      };

      async function jsonRequest(method, path, payload) {
        const response = await fetch(path, {
          method,
          credentials: "include",
          headers,
          body: payload ? JSON.stringify(payload) : undefined,
        });
        const text = await response.text();
        const data = text ? JSON.parse(text) : null;
        if (!response.ok) {
          throw new Error(`${method} ${path} failed: ${text}`);
        }
        return data;
      }

      async function getOrCreateDatabase() {
        const payload = await jsonRequest("GET", "/api/v1/database/");
        const databasePayload = {
          database_name: registry.database.name,
          sqlalchemy_uri: registry.database.sqlalchemy_uri,
          expose_in_sqllab: true,
          allow_ctas: false,
          allow_cvas: false,
          allow_dml: true,
          allow_file_upload: false,
          impersonate_user: false,
          configuration_method: "sqlalchemy_form",
          extra: JSON.stringify({ allow_multi_catalog: true, disable_data_preview: true }),
        };
        for (const item of payload.result || []) {
          if (item.database_name === registry.database.name) {
            await jsonRequest("PUT", `/api/v1/database/${item.id}`, databasePayload);
            return item.id;
          }
        }

        const created = await jsonRequest("POST", "/api/v1/database/", databasePayload);
        if (created && created.id) {
          return created.id;
        }

        const reread = await jsonRequest("GET", "/api/v1/database/");
        for (const item of reread.result || []) {
          if (item.database_name === registry.database.name) {
            return item.id;
          }
        }
        throw new Error("Could not resolve database id");
      }

      async function getOrCreateDataset(databaseId, spec) {
        let response = null;
        for (let attempt = 1; attempt <= 8; attempt += 1) {
          try {
            response = await jsonRequest("POST", "/api/v1/dataset/get_or_create/", {
              database_id: databaseId,
              schema: spec.schema,
              table_name: spec.table_name,
              always_filter_main_dttm: false,
              normalize_columns: false,
            });
            break;
          } catch (error) {
            if (attempt === 8) {
              throw error;
            }
            await new Promise(resolve => setTimeout(resolve, 2000));
          }
        }
        let datasetId = null;
        if (response && response.result && response.result.table_id != null) {
          datasetId = Number(response.result.table_id);
        } else if (response && response.id != null) {
          datasetId = Number(response.id);
        }

        const datasets = await jsonRequest("GET", "/api/v1/dataset/");
        for (const item of datasets.result || []) {
          if (
            Number(item.id) === datasetId ||
            (item.database && Number(item.database.id) === Number(databaseId) &&
              item.schema === spec.schema &&
              item.table_name === spec.table_name)
          ) {
            return item;
          }
        }
        throw new Error(`Could not resolve dataset ${spec.schema}.${spec.table_name}`);
      }

      async function listDashboards() {
        const payload = await jsonRequest("GET", "/api/v1/dashboard/");
        return payload.result || [];
      }

      async function getOrCreateDashboard(surface) {
        const slug = surface.title.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
        for (const item of await listDashboards()) {
          if (item.dashboard_title === surface.title || item.slug === slug) {
            return item;
          }
        }

        const created = await jsonRequest("POST", "/api/v1/dashboard/", {
          dashboard_title: surface.title,
          slug,
          published: true,
          json_metadata: JSON.stringify({
            color_scheme: "supersetColors",
            expanded_slices: {},
            refresh_frequency: surface.refresh_frequency,
          }),
          position_json: JSON.stringify({
            ROOT_ID: { type: "ROOT", id: "ROOT_ID", children: ["GRID_ID"] },
            GRID_ID: { type: "GRID", id: "GRID_ID", children: ["TABS_ID"] },
            TABS_ID: { type: "TABS", id: "TABS_ID", children: [] },
          }),
          css: "",
        });
        if (created && created.id) {
          const reread = await listDashboards();
          for (const item of reread) {
            if (Number(item.id) === Number(created.id)) {
              return item;
            }
          }
          if (created.dashboard_title) {
            return created;
          }
        }
        throw new Error(`Failed to create dashboard ${surface.title}`);
      }

      async function listCharts() {
        const payload = await jsonRequest("GET", "/api/v1/chart/");
        return payload.result || [];
      }

      async function getChartByName(name) {
        for (const item of await listCharts()) {
          if (item.slice_name === name) {
            return item;
          }
        }
        return null;
      }

      async function createChart(dataset, dashboardId, chartSpec) {
        const existing = await getChartByName(chartSpec.title);
        if (existing) {
          return existing;
        }

        const params = {
          datasource: `${dataset.id}__table`,
          viz_type: chartSpec.viz_type,
          query_mode: chartSpec.viz_type === "table" ? "raw" : "aggregate",
          slice_id: null,
          ...(chartSpec.form_data || {}),
        };
        const created = await jsonRequest("POST", "/api/v1/chart/", {
          slice_name: chartSpec.title,
          datasource_id: Number(dataset.id),
          datasource_type: "table",
          viz_type: chartSpec.viz_type,
          dashboards: [dashboardId],
          params: JSON.stringify(params),
          query_context_generation: true,
          description:
            chartSpec.description ||
            `Auto-generated ${chartSpec.viz_type} chart for ${dataset.table_name}`,
        });
        if (!created || !created.id) {
          throw new Error(`Failed to create chart ${chartSpec.title}`);
        }
        return created;
      }

      async function getChartDetail(chartId) {
        const payload = await jsonRequest("GET", `/api/v1/chart/${chartId}`);
        return payload && payload.result ? payload.result : payload;
      }

      function buildPositionJson(surface, chartsByTitle) {
        const position = {
          ROOT_ID: { type: "ROOT", id: "ROOT_ID", children: ["GRID_ID"] },
          GRID_ID: { type: "GRID", id: "GRID_ID", children: ["TABS_ID"] },
          TABS_ID: { type: "TABS", id: "TABS_ID", children: [] },
        };

        surface.sections.forEach((section, sectionIndex) => {
          const tabId = `TAB-${sectionIndex + 1}`;
          const tabChildren = [];
          section.charts.forEach((chartSpec, chartIndex) => {
            const detail = chartsByTitle[chartSpec.title];
            const nodeId = `${tabId}-CHART-${chartIndex + 1}`;
            tabChildren.push(nodeId);
            position[nodeId] = {
              type: "CHART",
              id: nodeId,
              children: [],
              meta: {
                chartId: Number(detail.id),
                uuid: detail.uuid || `chart-${detail.id}`,
                sliceName: detail.slice_name,
                width: chartSpec.width,
                height: chartSpec.height,
              },
            };
          });
          position.TABS_ID.children.push(tabId);
          position[tabId] = {
            type: "TAB",
            id: tabId,
            children: tabChildren,
            meta: {
              text: section.title,
              title: section.title,
            },
          };
        });

        return JSON.stringify(position);
      }

      async function updateDashboardLayout(dashboard, surface, chartsByTitle) {
        const payload = {
          dashboard_title: dashboard.dashboard_title,
          slug: dashboard.slug,
          published: true,
          json_metadata: JSON.stringify({
            color_scheme: "supersetColors",
            expanded_slices: {},
            refresh_frequency: surface.refresh_frequency,
          }),
          position_json: buildPositionJson(surface, chartsByTitle),
          css: dashboard.css || "",
          owners: (dashboard.owners || [])
            .filter((owner) => owner && owner.id)
            .map((owner) => owner.id),
        };

        const response = await fetch(`/api/v1/dashboard/${dashboard.id}`, {
          method: "PUT",
          headers,
          body: JSON.stringify(payload),
        });
        if (!response.ok) {
          throw new Error(`Failed to update dashboard ${dashboard.dashboard_title}`);
        }
      }

      const databaseId = await getOrCreateDatabase();
      for (const surface of registry.surfaces || []) {
        const dashboard = await getOrCreateDashboard(surface);
        const chartsByTitle = {};
        for (const section of surface.sections || []) {
          for (const chartSpec of section.charts || []) {
            const datasetSpec = (registry.datasets || []).find(
              (item) => item.name === chartSpec.dataset_name,
            );
            if (!datasetSpec) {
              throw new Error(`Missing dataset spec for ${chartSpec.dataset_name}`);
            }
            const dataset = await getOrCreateDataset(databaseId, datasetSpec);
            const chart = await createChart(dataset, dashboard.id, {
              title: chartSpec.title,
              viz_type: chartSpec.viz_type,
              form_data: chartSpec.form_data || {},
              description: chartSpec.description || null,
            });
            chartsByTitle[chartSpec.title] = await getChartDetail(chart.id);
          }
        }
        await updateDashboardLayout(dashboard, surface, chartsByTitle);
      }
    },
    { registry },
  );
}
