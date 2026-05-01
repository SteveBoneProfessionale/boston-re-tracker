import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


STATUS_COLORS = {
    "Under Review": "#f59e0b",
    "Board Approved": "#10b981",
    "Letter of Intent": "#3b82f6",
    "Under Construction": "#ef4444",
}


def render(df: pd.DataFrame, stats: dict):
    st.markdown("## Boston Article 80 Development Pipeline")
    st.caption(f"Tracking {stats['total']} active Article 80 projects · "
               f"{stats['extracted']} of {stats['total']} AI-extracted")

    kpis = [
        ("Total Projects",      f"{stats['total']:,}",                          "#3b82f6"),
        ("Under Review",        f"{stats['under_review']:,}",                   "#f59e0b"),
        ("Board Approved",      f"{stats['board_approved']:,}",                 "#10b981"),
        ("Letter of Intent",    f"{stats['loi']:,}",                            "#6366f1"),
        ("Under Construction",  f"{stats['under_construction']:,}",             "#ef4444"),
        ("Residential Units",   f"{stats['total_units']:,}",                    "#8b5cf6"),
        ("Total GSF",           f"{stats['total_gsf'] / 1_000_000:.1f}M ft²",  "#06b6d4"),
    ]
    cards = "".join(
        f"""<div style="flex:1;min-width:160px;background:#1c1f2e;border-radius:8px;
                        padding:20px 24px 18px 24px;border-top:3px solid {color}">
              <div style="font-size:2.2rem;font-weight:700;color:#f1f5f9;line-height:1.1;
                          white-space:nowrap">{value}</div>
              <div style="font-size:0.72rem;color:#94a3b8;text-transform:uppercase;
                          letter-spacing:0.09em;margin-top:8px;white-space:nowrap">{label}</div>
            </div>"""
        for label, value, color in kpis
    )
    st.markdown(
        f'<div style="display:flex;gap:14px;margin-bottom:12px;flex-wrap:wrap">{cards}</div>',
        unsafe_allow_html=True,
    )

    st.divider()

    # ── Row 1: neighborhood + status ───────────────────────────────────────
    col_a, col_b = st.columns([3, 2])

    with col_a:
        st.markdown("#### Projects by Neighborhood")
        nbhd = (
            df.groupby("neighborhood").size()
            .reset_index(name="count")
            .sort_values("count", ascending=True)
            .tail(15)
        )
        fig = px.bar(
            nbhd, x="count", y="neighborhood", orientation="h",
            color="count", color_continuous_scale="Blues",
            labels={"count": "Projects", "neighborhood": ""},
        )
        fig.update_layout(
            height=420, margin=dict(l=0, r=20, t=10, b=10),
            coloraxis_showscale=False,
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(size=12),
        )
        fig.update_xaxes(gridcolor="#e5e7eb")
        fig.update_yaxes(gridcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.markdown("#### Pipeline Status")
        status_df = df["status"].value_counts().reset_index()
        status_df.columns = ["status", "count"]
        status_df["color"] = status_df["status"].map(STATUS_COLORS)
        fig2 = px.pie(
            status_df, values="count", names="status",
            color="status", color_discrete_map=STATUS_COLORS,
            hole=0.5,
        )
        fig2.update_traces(textposition="outside", textinfo="label+value")
        fig2.update_layout(
            height=420, margin=dict(l=0, r=0, t=10, b=10),
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig2, use_container_width=True)

    # ── Row 2: asset class + scale (only show when extraction has data) ─────
    extracted = df[df["extraction_done"] & (df["asset_class"] != "")]

    col_c, col_d = st.columns(2)

    with col_c:
        st.markdown("#### Asset Class Mix")
        if len(extracted) >= 5:
            ac = extracted.groupby("asset_class").size().reset_index(name="count").sort_values("count", ascending=False)
            fig3 = px.bar(
                ac, x="asset_class", y="count",
                color="asset_class",
                labels={"asset_class": "", "count": "Projects"},
            )
            fig3.update_layout(
                height=300, margin=dict(l=0, r=0, t=10, b=10),
                showlegend=False,
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            )
            fig3.update_yaxes(gridcolor="#e5e7eb")
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info(f"Asset class data populates as extraction completes ({len(extracted)} done so far).")

    with col_d:
        st.markdown("#### Large vs Small Project Review")
        scale_df = df["project_scale"].value_counts().reset_index()
        scale_df.columns = ["scale", "count"]
        fig4 = px.pie(
            scale_df, values="count", names="scale",
            color_discrete_sequence=["#6366f1", "#a78bfa"],
            hole=0.5,
        )
        fig4.update_traces(textposition="outside", textinfo="label+value")
        fig4.update_layout(
            height=300, margin=dict(l=0, r=0, t=10, b=10),
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig4, use_container_width=True)

    # ── Top projects by GSF ────────────────────────────────────────────────
    if len(extracted) >= 5:
        st.divider()
        col_e, col_f = st.columns([3, 2])

        with col_e:
            st.markdown("#### Largest Projects by GSF")
            top_cols = extracted[extracted["total_gsf"].notna()].nlargest(10, "total_gsf").copy()
            top_cols["_dev"] = top_cols["developer_canonical"].where(
                top_cols["developer_canonical"].astype(bool), top_cols["developer"]
            )
            top = top_cols[["name", "neighborhood", "_dev", "total_gsf", "status"]].copy()
            top["total_gsf"] = top["total_gsf"].apply(lambda x: f"{int(x):,}")
            top.columns = ["Project", "Neighborhood", "Developer", "GSF", "Status"]
            st.dataframe(top, use_container_width=True, hide_index=True)

        with col_f:
            st.markdown("#### Most Active Developers")
            from scraper.normalize_developer import is_real_company
            dev_df = df[df["developer_canonical"].apply(
                lambda x: bool(x) and is_real_company(str(x))
            )].copy()
            if len(dev_df) >= 3:
                dev_counts = (
                    dev_df.groupby("developer_canonical").size()
                    .reset_index(name="projects")
                    .sort_values("projects", ascending=True)
                    .tail(12)
                )
                fig5 = px.bar(
                    dev_counts, x="projects", y="developer_canonical", orientation="h",
                    color="projects", color_continuous_scale="Purples",
                    labels={"projects": "Projects", "developer_canonical": ""},
                )
                fig5.update_layout(
                    height=380, margin=dict(l=0, r=20, t=10, b=10),
                    coloraxis_showscale=False,
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(size=11),
                )
                fig5.update_xaxes(gridcolor="#e5e7eb")
                fig5.update_yaxes(gridcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig5, use_container_width=True)
