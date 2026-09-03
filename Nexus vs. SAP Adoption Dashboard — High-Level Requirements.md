## Nexus vs. SAP Adoption Dashboard — High-Level Requirements

The goal is to build a Power BI dashboard that shows **Nexus adoption compared with SAP usage** and allows leadership to quickly identify where adoption is strong or weak across the organization.

### 1. Organize the data around the reporting hierarchy

The primary hierarchy should be:

**Manager → Supervisor → User**

The report should allow users to start at the organizational level and progressively drill into the people contributing to the results.

### 2. Build the adoption metrics correctly

The primary metric is **Nexus Adoption %**, with SAP usage providing the comparison.

Where usage counts are available, adoption should be calculated from the underlying Nexus and SAP activity rather than averaging percentages.

We also want supporting metrics such as:

- Nexus Adoption %
- SAP Adoption %
- Active Users
- Adoption Target
- Gap to Target
- Users meeting the target
- Users below the target

### 3. Create an executive overview

The main page should quickly answer:

- What is our overall Nexus adoption?
- Are we above or below target?
- Which managers have the highest and lowest adoption?
- Which supervisors are contributing to those results?
- How many users are below the desired adoption level?

The focus should be on a small number of useful KPIs and ranked comparisons rather than trying to show all available data.

### 4. Support hierarchical analysis

Selecting or drilling into a manager should show the supervisors underneath that manager.

Selecting or drilling into a supervisor should expose the individual users contributing to that supervisor's adoption rate.

The main analytical flow should feel like:

**Overall Adoption → Manager → Supervisor → User**

The goal is to make it easy to move from identifying a problem to identifying who or what is contributing to it.

### 5. Provide detailed user-level analysis

At the lowest level, provide a user-detail view showing adoption, usage, gap to target, and organizational context.

Users with the lowest Nexus adoption should be easy to identify so leadership or managers can use the report as an actionable follow-up list.

### 6. Include trend analysis if historical data is available

If we have adoption data over time, include a trend view showing whether Nexus adoption is improving or declining.

Users should be able to filter trends by manager, supervisor, or other relevant organizational dimensions.

### 7. Use a clean Power BI semantic model

Structure the Power BI model appropriately for production reporting, ideally around:

- Organizational hierarchy/dimension
- Adoption usage fact data
- Date dimension where historical reporting is required

The model should support correct aggregation and filtering at every organizational level.

### 8. Make interactions intuitive

Filters, slicers, cross-filtering, and drill-through should work naturally so that the selected organizational context carries through the report.

The dashboard should always make it obvious whether the user is looking at the entire organization, a manager, a supervisor, or an individual user.

### 9. Keep the visual design focused

Prioritize:

**Nexus Adoption → Gap to Target → Manager Ranking → Supervisor Ranking → User Detail**

Use consistent visual indicators for high, medium, and low adoption so problem areas are immediately recognizable.

### 10. Validate before publishing

Before release, verify that:

- Users map correctly to supervisors and managers.
- Adoption calculations reconcile with the source data.
- Manager and supervisor adoption aggregates correctly.
- Filters and drill-through preserve the correct organizational context.
- Published data refresh and access permissions work as expected.

## End Goal

This should not just be a static reporting dashboard.

It should function as a **diagnostic tool** where leadership can see overall Nexus adoption, identify an underperforming area, drill down through the organization, and ultimately determine which users are driving the result.