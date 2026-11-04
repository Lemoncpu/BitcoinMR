// Executes the real page script against captured HBase responses with a minimal DOM/chart adapter.
// This checks switching, filtering and ECharts option generation; it is not a visual browser test.
const fs = require("fs"),
  vm = require("vm"),
  assert = require("assert");
const html = fs.readFileSync("mr7_echarts.html", "utf8");
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
class Element {
  constructor(id) {
    this.id = id;
    this.children = [];
    this.style = {};
    this.value = "";
    this.handlers = {};
    this.content = "";
  }
  set textContent(value) {
    this.content = String(value);
    this.children = [];
  }
  get textContent() {
    return (
      this.content + this.children.map((child) => child.textContent).join("")
    );
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  replaceChildren(...children) {
    this.content = "";
    this.children = children;
  }
  addEventListener(event, handler) {
    this.handlers[event] = handler;
  }
}
const ids = [...html.matchAll(/id="([^"]+)"/g)].map((match) => match[1]);
const elements = Object.fromEntries(ids.map((id) => [id, new Element(id)]));
elements.jobSelector.value = "mr7";
elements.platform.value = "alpha";
elements.axisScale.value = "auto";
const chartInstances = {};
function checkValues(value) {
  if (typeof value === "number")
    assert(Number.isFinite(value), "Non-finite chart value");
  else if (Array.isArray(value)) value.forEach(checkValues);
  else if (value && typeof value === "object")
    Object.values(value).forEach(checkValues);
}
const context = {
  console,
  document: {
    getElementById: (id) => {
      assert(elements[id], "Unknown DOM id " + id);
      return elements[id];
    },
    createElement: (tag) => new Element(tag),
  },
  window: { addEventListener() {} },
  echarts: {
    init: (element) => {
      const chart = {
        setOption(option) {
          checkValues(option);
          this.option = option;
        },
        resize() {},
        clear() {
          this.option = null;
        },
        showLoading() {},
        hideLoading() {},
      };
      chartInstances[element.id] = chart;
      return chart;
    },
  },
  fetch: async (path) => {
    const url = new URL(path, "http://test.local");
    const job = url.searchParams.get("job") || "mr7",
      platform = url.searchParams.get("platform") || "alpha";
    assert(job !== "mr22" || platform === "otc", "MR22 must never query Alpha");
    const payload = JSON.parse(
      fs.readFileSync(`target/api-payloads/${job}-${platform}.json`, "utf8"),
    );
    return { ok: true, json: async () => payload };
  },
};
vm.createContext(context);
vm.runInContext(script, context);
async function flush() {
  await new Promise((resolve) => setImmediate(resolve));
}
(async () => {
  await flush();
  assert(
    !elements.status.textContent.includes("失败"),
    elements.status.textContent,
  );
  for (const platform of ["alpha", "otc"])
    for (const job of ["mr22", "mr26", "mr18", "mr25", "mr27", "mr7"]) {
      if (job === "mr22" && platform !== "otc") continue;
      elements.platform.value = platform;
      elements.jobSelector.value = job;
      context.render();
      await flush();
      assert(
        !elements.status.textContent.includes("失败"),
        `${job}/${platform}: ${elements.status.textContent}`,
      );
      assert(elements.status.textContent.includes("HBase"));
      if (job !== "mr7") {
        assert(chartInstances.analyticsPrimary.option);
        assert(chartInstances.analyticsSecondary.option);
        assert(elements.analyticsRows.children.length > 0);
        assert(elements.analyticsRows.children.length <= 10);
      }
      if (job !== "mr7")
        for (const chart of [
          chartInstances.analyticsPrimary,
          chartInstances.analyticsSecondary,
        ]) {
          const option = chart.option;
          if (
            option.yAxis?.max !== undefined &&
            typeof option.yAxis.max === "number" &&
            option.series[0].type === "bar"
          ) {
            for (const series of option.series)
              for (const item of series.data) {
                const value = typeof item === "object" ? item.value : item;
                assert(value <= option.yAxis.max, "Bar exceeds axis maximum");
              }
          }
        }
      if (job === "mr27") {
        const option = chartInstances.analyticsSecondary.option.series[0];
        const ids = new Set(option.data.map((row) => row.id));
        assert(ids.size === option.data.length && ids.size <= 22);
        for (const edge of option.links)
          assert(ids.has(edge.source) && ids.has(edge.target));
        assert(option.links.filter((edge) => edge.value < 0).length === 1);
      }
      if (job === "mr25") {
        const payload = JSON.parse(
          fs.readFileSync(`target/api-payloads/mr25-${platform}.json`, "utf8"),
        );
        const cells = payload.datasets.cells;
        const option = chartInstances.analyticsPrimary.option;
        const contributions = chartInstances.analyticsSecondary.option;
        assert(option.series.length === 3 && contributions.series.length === 3);
        assert(
          option.yAxis.max === undefined,
          "Participation axis must follow actual values",
        );
        for (const [index, horizon] of [1, 3, 6].entries()) {
          for (const [position, cohort] of option.xAxis.data.entries()) {
            const row = cells.find(
              (row) =>
                row.cohort_month === cohort &&
                Number(row.month_offset) === horizon,
            );
            const eligible = row.comparison_eligible === "true";
            assert(
              eligible
                ? Number(row.cohort_size) >= 30 &&
                    row.observation_status === "observed"
                : true,
            );
            assert.strictEqual(
              option.series[index].data[position],
              eligible ? Number(row.activity_rate) * 100 : null,
            );
            assert.strictEqual(
              contributions.series[index].data[position],
              eligible ? Number(row.event_count) : null,
            );
          }
        }
        for (const row of cells.filter((row) => row.month_offset === "0")) {
          assert(Number(row.active_users) === Number(row.cohort_size));
          assert(Number(row.activity_rate) === 1);
        }
      }
      if (job === "mr7") {
        assert(
          chartInstances.groupedBarChart.option.yAxis.name.includes("原始名次"),
        );
        assert(
          chartInstances.groupedBarChart.option.yAxis.name.includes("对数压缩"),
        );
      }
      console.log(job, platform, "switch and chart options PASS");
    }
  assert(
    !elements.userFilter && !elements.applyFilter,
    "User query controls must be removed",
  );
  console.log("User ID query controls removed PASS");
  for (const mode of ["linear", "log", "auto"]) {
    elements.axisScale.value = mode;
    const option = context.scaledBars({
      yAxis: { name: "测试" },
      series: [{ type: "bar", data: [0, 1, 100000] }],
    });
    assert(option.yAxis.min === 0);
    for (const item of option.series[0].data)
      assert(item.value <= option.yAxis.max);
    assert(option.yAxis.name.includes("对数压缩") === (mode !== "linear"));
    console.log(mode, "axis-range policy PASS");
  }
  elements.platform.value = "alpha";
  elements.jobSelector.value = "mr22";
  context.render();
  await flush();
  assert(elements.platform.value === "otc" && elements.platform.disabled);
  assert(elements.status.textContent.includes("OTC"));
  elements.jobSelector.value = "mr26";
  context.render();
  await flush();
  assert(elements.platform.value === "alpha" && !elements.platform.disabled);
  console.log("MR22 OTC lock and platform restoration PASS");
  const zeroOption = context.scaledBars({
    yAxis: { name: "测试" },
    series: [{ type: "bar", data: [0, 1, 100000] }],
  });
  assert(zeroOption.series[0].data[0].value === 0);
  assert(zeroOption.series[0].data[2].rawValue === 100000);
  assert(zeroOption.series[0].data[2].value < zeroOption.yAxis.max);
  console.log("Zero values and large extremes preserved PASS");
  fs.writeFileSync(
    "target/frontend-logic-verification.json",
    JSON.stringify(
      {
        passed: true,
        views: 11,
        userFilters: 0,
        scope: "DOM adapter and real HBase payloads; no visual rendering test",
      },
      null,
      2,
    ),
  );
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
