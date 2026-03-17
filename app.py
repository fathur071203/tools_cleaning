from __future__ import annotations
# pyright: reportMissingImports=false

from io import BytesIO
import re
import pandas as pd
import streamlit as st

from olah import (
    excel_col_to_index,
    process_ltdbb_cleaner,
    process_perlindungan_konsumen,
    process_rencana_edukasi,
    process_realisasi_edukasi,
)

st.set_page_config(page_title="Data Cleaning PK", page_icon="📊", layout="wide")
st.markdown(
    """
    <style>
    :root {
        --bg-main: #f6f8fc;
        --bg-card: #ffffff;
        --text-main: #13233a;
        --text-soft: #5a6b86;
        --primary: #1663ff;
        --primary-2: #00a7c2;
        --border-soft: #e6ecf7;
        --ok: #20b26b;
    }
    .stApp {
        background:
            radial-gradient(circle at 10% -10%, rgba(22,99,255,0.12), transparent 35%),
            radial-gradient(circle at 100% 0%, rgba(0,167,194,0.12), transparent 30%),
            var(--bg-main);
        color: var(--text-main);
    }
    /* Pastikan top bar official Streamlit tetap terlihat jelas (Deploy/Rerun/menu) */
    header[data-testid="stHeader"] {
        background: rgba(255, 255, 255, 0.92) !important;
        backdrop-filter: blur(6px);
        border-bottom: 1px solid var(--border-soft);
    }
    div[data-testid="stToolbar"] {
        right: 0.75rem;
    }
    div[data-testid="stToolbar"] button,
    div[data-testid="stToolbar"] [role="button"] {
        border-radius: 8px !important;
    }
    div[data-testid="stDecoration"] {
        background: linear-gradient(90deg, #1663ff 0%, #00a7c2 100%) !important;
        height: 3px !important;
    }
    [data-testid="stStatusWidget"] {
        color: var(--text-main) !important;
    }
    .block-container {
        /* Ruang aman agar tidak ketabrak App Toolbar (Deploy/Rerun) */
        padding-top: 4.1rem;
        padding-bottom: 0.8rem;
        padding-left: 1rem;
        padding-right: 1rem;
        max-width: 100%;
    }
    h1, h2, h3 {
        color: var(--text-main) !important;
        letter-spacing: 0.2px;
    }
    p, .stCaption {
        color: var(--text-soft) !important;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #ffffff 0%, #f8fbff 100%);
        border: 1px solid var(--border-soft);
        border-radius: 12px;
        padding: 0.35rem 0.55rem;
        box-shadow: 0 2px 10px rgba(18, 38, 63, 0.05);
    }
    div[data-testid="stMetricLabel"] p {
        color: var(--text-soft) !important;
        font-weight: 600;
    }
    div[data-testid="stMetricValue"] {
        color: var(--text-main) !important;
        font-weight: 800;
    }
    div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stAlert"]) {
        margin-bottom: 0.35rem;
    }
    [data-testid="stSidebar"] .block-container {
        padding-top: 0.8rem;
        padding-left: 0.7rem;
        padding-right: 0.7rem;
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f2f66 0%, #0f4ea6 45%, #1264b0 100%);
    }
    [data-testid="stSidebar"] * {
        color: #f4f8ff !important;
    }
    [data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] > div,
    [data-testid="stSidebar"] .stTextInput input {
        background: rgba(255, 255, 255, 0.12) !important;
        border: 1px solid rgba(255, 255, 255, 0.35) !important;
        color: #ffffff !important;
    }
    [data-testid="stSidebar"] .stTextInput input::placeholder {
        color: #e1ecff !important;
    }
    .stButton button, .stDownloadButton button {
        padding-top: 0.35rem !important;
        padding-bottom: 0.35rem !important;
        border-radius: 10px !important;
        font-weight: 700 !important;
        border: none !important;
    }
    .stButton button[kind="primary"] {
        background: linear-gradient(90deg, var(--primary) 0%, var(--primary-2) 100%) !important;
        color: #ffffff !important;
        box-shadow: 0 6px 16px rgba(16, 83, 203, 0.32);
    }
    .stDownloadButton button {
        background: linear-gradient(90deg, #12a150 0%, #20b26b 100%) !important;
        color: #ffffff !important;
        box-shadow: 0 5px 14px rgba(32, 178, 107, 0.28);
    }
    div[data-testid="stAlert"] {
        border-radius: 10px;
        border: 1px solid var(--border-soft);
    }
    [data-testid="stExpander"] {
        border-radius: 12px !important;
        border: 1px solid var(--border-soft) !important;
        background: var(--bg-card) !important;
    }
    [data-testid="stDataFrame"] {
        border: 1px solid var(--border-soft);
        border-radius: 10px;
        overflow: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
MODE_MAP = {
    "Perlindungan Konsumen": "perlindungan-konsumen",
    "Rencana Edukasi Konsumen": "rencana-edukasi",
    "Realisasi Edukasi Publik": "realisasi-edukasi-publik",
}

TOOL_LABEL_PK = "Data Cleaning PK"
TOOL_LABEL_LTDBB = "Cleaner Template LTDBB"
TOOL_META = {
    TOOL_LABEL_PK: {
        "button": "📊 Perlindungan",
        "title": "Data Cleaning PK",
        "caption": "3 mode cleaning Excel",
        "subtitle": "Cleaning data Perlindungan Konsumen, Rencana Edukasi, dan Realisasi Edukasi.",
    },
    TOOL_LABEL_LTDBB: {
        "button": "🧹 LTDBB",
        "title": "Cleaner LTDBB",
        "caption": "Template CSV/XLS/XLSX",
        "subtitle": "Konversi template LTDBB mentah menjadi dataset bersih siap analisis.",
    },
}


def parse_sheet_value(raw: str) -> int | str:
    value = raw.strip()
    if value == "":
        return 0
    if value.isdigit():
        return int(value)
    return value


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer.getvalue()


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def _norm_header(text: object) -> str:
    s = str(text).casefold().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def detect_perlindungan_columns(df: pd.DataFrame) -> tuple[int | str, int | str, int | str, dict[str, str]]:
    """
    Deteksi otomatis kolom untuk mode Perlindungan Konsumen.
    Prioritas: cari berdasarkan nama header, fallback ke index 0/1/2.
    """
    headers = list(df.columns)
    header_keys = [_norm_header(h) for h in headers]

    def _find_by_patterns(patterns: list[str]) -> str | None:
        for i, hk in enumerate(header_keys):
            for p in patterns:
                if re.search(p, hk):
                    return headers[i]
        return None

    nama_col = _find_by_patterns([
        r"\bnama\s*penyelenggara\b",
        r"\bnama\s*pt\b",
        r"\bpenyelenggara\b",
    ])
    waktu_col = _find_by_patterns([
        r"\bcompletion\s*time\b",
        r"\bwaktu\b",
        r"\btanggal\b",
        r"\bsubmitted?\b",
    ])
    satker_col = _find_by_patterns([
        r"\bsatuan\s*kerja\s*bank\s*indonesia\b",
        r"\bkpwbi\b",
        r"\bsatuan\s*kerja\b",
    ])

    # fallback berbasis posisi jika tidak ketemu header yang cocok
    col_a: int | str = nama_col if nama_col is not None else (0 if len(headers) > 0 else "0")
    col_b: int | str = waktu_col if waktu_col is not None else (1 if len(headers) > 1 else "1")
    col_c: int | str = satker_col if satker_col is not None else (2 if len(headers) > 2 else "2")

    info = {
        "nama": str(col_a),
        "completion": str(col_b),
        "satker": str(col_c),
    }
    return col_a, col_b, col_c, info


def render_hero(title: str, subtitle: str, caption: str) -> None:
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, #0f4ea6 0%, #1663ff 55%, #00a7c2 100%);
            border-radius: 14px;
            padding: 14px 16px;
            margin-bottom: 10px;
            box-shadow: 0 8px 22px rgba(20, 74, 182, 0.28);
            color: white;
        ">
            <div style="font-size: 22px; font-weight: 800; line-height: 1.15;">{title}</div>
            <div style="font-size: 13px; opacity: 0.95; margin-top: 3px;">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(caption)


def render_sidebar_tool_nav() -> str:
    if "selected_tool" not in st.session_state:
        st.session_state["selected_tool"] = TOOL_LABEL_PK

    current_tool = st.session_state["selected_tool"]

    st.markdown("### Navigasi Fitur")
    nav_col1, nav_col2 = st.columns(2)
    with nav_col1:
        pk_clicked = st.button(
            TOOL_META[TOOL_LABEL_PK]["button"],
            use_container_width=True,
            type="primary" if current_tool == TOOL_LABEL_PK else "secondary",
            key="nav_tool_pk",
        )
    with nav_col2:
        ltdbb_clicked = st.button(
            TOOL_META[TOOL_LABEL_LTDBB]["button"],
            use_container_width=True,
            type="primary" if current_tool == TOOL_LABEL_LTDBB else "secondary",
            key="nav_tool_ltdbb",
        )

    if pk_clicked:
        current_tool = TOOL_LABEL_PK
        st.session_state["selected_tool"] = current_tool
    elif ltdbb_clicked:
        current_tool = TOOL_LABEL_LTDBB
        st.session_state["selected_tool"] = current_tool

    active_meta = TOOL_META[current_tool]
    st.markdown(
        f"""
        <div style="
            margin-top: 0.55rem;
            padding: 0.75rem 0.8rem;
            border-radius: 12px;
            background: rgba(255,255,255,0.12);
            border: 1px solid rgba(255,255,255,0.24);
        ">
            <div style="font-size: 0.78rem; opacity: 0.85; margin-bottom: 0.15rem;">Fitur aktif</div>
            <div style="font-size: 1rem; font-weight: 800; line-height: 1.2;">{active_meta['title']}</div>
            <div style="font-size: 0.8rem; opacity: 0.92; margin-top: 0.15rem;">{active_meta['caption']}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.divider()
    return current_tool


def open_tool(tool_label: str) -> None:
    st.session_state["selected_tool"] = tool_label
    st.session_state["current_view"] = "tool"


def go_home() -> None:
    st.session_state["current_view"] = "home"


def render_home_page() -> None:
    render_hero(
        "🧰 Kumpulan Tools Data Cleaning",
        "Pilih tool yang ingin digunakan sebelum masuk ke halaman proses",
        "Mulai dari halaman ini, lalu masuk ke tool yang dibutuhkan.",
    )

    st.markdown(
        """
        <div style="margin: 0.2rem 0 1rem 0; color: #5a6b86; font-size: 0.97rem;">
            Tersedia dua tool utama untuk pengolahan data. Pilih salah satu tombol di bawah untuk masuk ke halaman tool.
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    for column, tool_label, accent in [
        (col1, TOOL_LABEL_PK, "#1663ff"),
        (col2, TOOL_LABEL_LTDBB, "#12a150"),
    ]:
        meta = TOOL_META[tool_label]
        with column:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
                    border: 1px solid #e6ecf7;
                    border-radius: 16px;
                    padding: 1rem 1rem 0.85rem 1rem;
                    min-height: 190px;
                    box-shadow: 0 6px 18px rgba(18, 38, 63, 0.06);
                ">
                    <div style="display:inline-block; padding: 0.28rem 0.55rem; border-radius: 999px; background: {accent}18; color: {accent}; font-size: 0.78rem; font-weight: 700; margin-bottom: 0.8rem;">
                        {meta['caption']}
                    </div>
                    <div style="font-size: 1.2rem; font-weight: 800; color: #13233a; margin-bottom: 0.4rem;">
                        {meta['title']}
                    </div>
                    <div style="font-size: 0.94rem; color: #5a6b86; line-height: 1.5; margin-bottom: 1rem;">
                        {meta['subtitle']}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button(f"Masuk {meta['button']}", use_container_width=True, key=f"home_open_{tool_label}"):
                open_tool(tool_label)

    st.info("Tips: setelah masuk ke tool, Anda tetap bisa kembali ke halaman beranda dari sidebar atau tombol di atas halaman.")


def read_ltdbb_source(uploaded_file) -> pd.DataFrame:
    raw_bytes = uploaded_file.getvalue()
    file_name = uploaded_file.name.casefold()

    if file_name.endswith(".csv"):
        last_error: Exception | None = None
        for encoding in ["utf-8-sig", "utf-8", "latin1"]:
            try:
                return pd.read_csv(
                    BytesIO(raw_bytes),
                    header=None,
                    dtype=object,
                    sep=None,
                    engine="python",
                    encoding=encoding,
                )
            except Exception as exc:
                last_error = exc
        raise ValueError("File CSV tidak dapat dibaca. Pastikan delimiter dan encoding valid.") from last_error

    try:
        return pd.read_excel(BytesIO(raw_bytes), header=None, dtype=object)
    except ImportError as exc:
        raise ValueError(
            "File Excel lama (.xls) membutuhkan paket `xlrd`. Tambahkan ke environment lalu coba lagi."
        ) from exc


def build_ltdbb_top_destinations(
    cleaned_df: pd.DataFrame,
    variant: str | None,
) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    dest_col = None
    if variant == "G0001":
        dest_col = "Negara Tujuan Pengiriman"
    elif variant in {"G0002", "G0003"}:
        dest_col = "Kota/Kab. Tujuan Pengiriman"
    elif "Negara Tujuan Pengiriman" in cleaned_df.columns:
        dest_col = "Negara Tujuan Pengiriman"
    elif "Kota/Kab. Tujuan Pengiriman" in cleaned_df.columns:
        dest_col = "Kota/Kab. Tujuan Pengiriman"

    if dest_col is None or dest_col not in cleaned_df.columns:
        return pd.DataFrame(), pd.DataFrame(), None

    required_cols = {"Frekuensi Pengiriman", "Total Nominal Transaksi"}
    if not required_cols.issubset(set(cleaned_df.columns)):
        return pd.DataFrame(), pd.DataFrame(), dest_col

    grouped = (
        cleaned_df.groupby(dest_col, dropna=True, as_index=False)[["Frekuensi Pengiriman", "Total Nominal Transaksi"]]
        .sum()
        .rename(columns={dest_col: "Destinasi"})
    )
    grouped["Frekuensi Pengiriman"] = pd.to_numeric(grouped["Frekuensi Pengiriman"], errors="coerce").fillna(0)
    grouped["Total Nominal Transaksi"] = pd.to_numeric(grouped["Total Nominal Transaksi"], errors="coerce").fillna(0)

    top_freq = grouped.sort_values(["Frekuensi Pengiriman", "Total Nominal Transaksi"], ascending=False).head(10).reset_index(drop=True)
    top_nominal = grouped.sort_values(["Total Nominal Transaksi", "Frekuensi Pengiriman"], ascending=False).head(10).reset_index(drop=True)
    return top_freq, top_nominal, dest_col


def render_pk_tool(selected_label: str, selected_mode: str, sheet_input: str, force_prefix_pt: bool) -> None:
    uploaded = st.file_uploader("Upload file Excel (.xlsx)", type=["xlsx"], key="pk_upload")

    if uploaded is not None:
        c_file, c_btn = st.columns([3, 1])
        with c_file:
            st.success(f"File aktif: {uploaded.name}")
        with c_btn:
            st.caption("Siap diproses")

    run_clicked = st.button(
        "⚡ Proses Data",
        type="primary",
        use_container_width=True,
        disabled=(uploaded is None),
        key="pk_process",
    )

    if not run_clicked:
        return

    try:
        sheet_value = parse_sheet_value(sheet_input)
        df_input = pd.read_excel(uploaded, sheet_name=sheet_value)

        if selected_mode == "rencana-edukasi":
            if sheet_value == 0 and len(df_input.columns) < excel_col_to_index("CH") + 1:
                try:
                    uploaded.seek(0)
                    df_input = pd.read_excel(uploaded, sheet_name="Form1")
                except Exception:
                    pass
            df_out = process_rencana_edukasi(df_input, force_prefix_pt=force_prefix_pt)
            out_suffix = "database_rencana_edukasi"

        elif selected_mode == "realisasi-edukasi-publik":
            df_out = process_realisasi_edukasi(df_input, force_prefix_pt=force_prefix_pt)
            out_suffix = "database_realisasi_edukasi"

        else:
            col_a, col_b, col_c, detected = detect_perlindungan_columns(df_input)
            df_out = process_perlindungan_konsumen(
                df_input,
                col_a=col_a,
                col_b=col_b,
                col_c=col_c,
                force_prefix_pt=force_prefix_pt,
            )
            out_suffix = "cleaned"

        output_bytes = to_excel_bytes(df_out)
        base_name = uploaded.name.rsplit(".", 1)[0]
        output_name = f"{base_name}__{out_suffix}.xlsx"

        m1, m2, m3 = st.columns(3)
        m1.metric("Mode", selected_label)
        m2.metric("Baris Input", f"{len(df_input):,}")
        m3.metric("Baris Output", f"{len(df_out):,}")

        if selected_mode == "perlindungan-konsumen":
            st.caption(
                "Deteksi kolom otomatis → "
                f"Nama: {detected['nama']} | Completion: {detected['completion']} | Satuan Kerja/KPwBI: {detected['satker']}"
            )

        d1, d2 = st.columns([1.2, 2.8])
        with d1:
            st.download_button(
                label="⬇️ Download Hasil",
                data=output_bytes,
                file_name=output_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with d2:
            st.caption(f"Nama file output: {output_name}")

        with st.expander("Preview data (200 baris pertama)", expanded=False):
            st.dataframe(df_out.head(200), use_container_width=True, height=420)
    except Exception as exc:
        st.error("Proses gagal. Cek konfigurasi kolom/sheet dan format file.")
        st.exception(exc)


def render_ltdbb_tool(variant_override: str | None) -> None:
    st.subheader("🧹 Bersihkan Template LTDBB LKPBU")
    st.write(
        "Hapus baris 1–5, gunakan baris ke-6 sebagai header, buang kolom A & B, bersihkan banner/footer, dan normalisasi kolom otomatis."
    )

    uploaded = st.file_uploader(
        "Upload file LTDBB (.csv, .xls, .xlsx)",
        type=["csv", "xls", "xlsx"],
        key="ltdbb_upload",
    )

    if uploaded is not None:
        c1, c2 = st.columns([3, 1])
        with c1:
            st.success(f"File aktif: {uploaded.name}")
        with c2:
            st.caption("Siap diproses")

    run_clicked = st.button(
        "🧹 Proses Template LTDBB",
        type="primary",
        use_container_width=True,
        disabled=(uploaded is None),
        key="ltdbb_process",
    )

    if not run_clicked:
        with st.expander("Tips", expanded=False):
            st.markdown(
                """
                - Upload file mentah langsung dari sistem sumber tanpa modifikasi manual.
                - Jika file `.xls` gagal dibaca, pastikan paket `xlrd` sudah terpasang.
                - Override jenis hanya dipakai jika auto-detect tidak tepat.
                - Ringkasan PJP/Periode diambil dari header sheet dan fallback ke 10 baris pertama.
                """
            )
        return

    try:
        df_raw = read_ltdbb_source(uploaded)
        cleaned_df, meta = process_ltdbb_cleaner(
            df_raw,
            filename=uploaded.name,
            variant_override=variant_override,
        )

        top_freq, top_nominal, dest_col = build_ltdbb_top_destinations(cleaned_df, meta.get("variant"))
        base_name = uploaded.name.rsplit(".", 1)[0]

        st.success("File berhasil diproses. Lihat ringkasan di bawah dan unduh CSV bersih.")

        action_cols = st.columns([1.2, 1.2, 1.2])
        with action_cols[0]:
            st.download_button(
                "⬇️ Unduh CSV Bersih",
                data=to_csv_bytes(cleaned_df),
                file_name=f"{base_name}__cleaned.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with action_cols[1]:
            st.download_button(
                "⬇️ Unduh Excel Bersih",
                data=to_excel_bytes(cleaned_df),
                file_name=f"{base_name}__cleaned.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with action_cols[2]:
            st.caption(f"Jenis terdeteksi: {meta.get('variant') or 'Auto tidak yakin'}")

        metric_row_1 = st.columns(4)
        metric_row_1[0].metric("Nama PJP", str(meta.get("pjp_name") or "-"))
        metric_row_1[1].metric("Sandi PJP", str(meta.get("pjp_sandi") or "-"))
        metric_row_1[2].metric("Periode", str(meta.get("periode_text") or "-"))
        metric_row_1[3].metric("Total Baris", f"{int(meta.get('rows') or 0):,}")

        metric_row_2 = st.columns(4)
        total_frekuensi = meta.get("total_frekuensi")
        total_nominal = meta.get("total_nominal")
        metric_row_2[0].metric(
            "Total Frekuensi",
            f"{int(total_frekuensi):,}" if total_frekuensi is not None else "-",
        )
        metric_row_2[1].metric(
            "Total Nominal",
            f"Rp {total_nominal:,.0f}" if total_nominal is not None else "-",
        )
        metric_row_2[2].metric("Jumlah Kolom", f"{len(cleaned_df.columns):,}")
        metric_row_2[3].metric(
            "Keterangan Jenis",
            {
                "G0001": "Outgoing",
                "G0002": "Ingoing",
                "G0003": "Domestik",
            }.get(meta.get("variant"), "-")
        )

        tab_preview, tab_freq, tab_nominal, tab_tips = st.tabs([
            "Preview",
            "Top Frekuensi",
            "Top Nominal",
            "Tips",
        ])

        with tab_preview:
            st.write("10 baris pertama hasil cleaning")
            st.dataframe(cleaned_df.head(10), use_container_width=True, height=420)

        with tab_freq:
            if top_freq.empty:
                st.info("Data top destinasi berdasarkan frekuensi belum tersedia.")
            else:
                st.write(f"Top 10 destinasi berdasarkan frekuensi ({dest_col})")
                st.download_button(
                    "⬇️ Export Top Frekuensi CSV",
                    data=to_csv_bytes(top_freq),
                    file_name=f"{base_name}__top_dest_freq.csv",
                    mime="text/csv",
                    use_container_width=False,
                    key="download_top_freq",
                )
                st.dataframe(top_freq, use_container_width=True, height=360)

        with tab_nominal:
            if top_nominal.empty:
                st.info("Data top destinasi berdasarkan nominal belum tersedia.")
            else:
                st.write(f"Top 10 destinasi berdasarkan nominal ({dest_col})")
                st.download_button(
                    "⬇️ Export Top Nominal CSV",
                    data=to_csv_bytes(top_nominal),
                    file_name=f"{base_name}__top_dest_nominal.csv",
                    mime="text/csv",
                    use_container_width=False,
                    key="download_top_nominal",
                )
                st.dataframe(top_nominal, use_container_width=True, height=360)

        with tab_tips:
            st.markdown(
                """
                - Upload file mentah langsung dari sistem sumber tanpa modifikasi manual.
                - Jika `.xls` lama gagal dibaca, pastikan `xlrd` terpasang sesuai requirements.
                - Pemetaan header dibuat toleran terhadap variasi spasi/pemisah umum.
                - Override jenis berguna jika file G0002/G0003 sulit dibedakan hanya dari kolom.
                """
            )
    except ValueError as exc:
        st.error(f"Kesalahan pemrosesan: {exc}")
    except Exception as exc:
        st.error(
            "File tidak dapat dibaca sebagai Excel/CSV. Pastikan format benar (CSV/XLSX/XLS) dan file tidak rusak."
        )
        st.exception(exc)


with st.sidebar:
    if "current_view" not in st.session_state:
        st.session_state["current_view"] = "home"

    current_view = st.session_state["current_view"]
    selected_tool = st.session_state.get("selected_tool", TOOL_LABEL_PK)

    if current_view == "tool":
        if st.button("← Kembali ke Beranda", use_container_width=True, key="sidebar_go_home"):
            go_home()
        st.divider()

        selected_tool = render_sidebar_tool_nav()
        st.subheader("Konfigurasi Ringkas")

        if selected_tool == TOOL_LABEL_PK:
            selected_label = st.selectbox("Jenis File", list(MODE_MAP.keys()))
            selected_mode = MODE_MAP[selected_label]

            c1, c2 = st.columns([1.1, 1])
            with c1:
                sheet_input = st.text_input("Sheet", value="0", help="Isi index (0,1,2) atau nama sheet.")
            with c2:
                force_prefix_pt = st.toggle("Force PT", value=True)
            st.caption("Mode Perlindungan Konsumen: kolom dideteksi otomatis.")
            ltdbb_variant_override = None
        else:
            selected_label = list(MODE_MAP.keys())[0]
            selected_mode = MODE_MAP[selected_label]
            sheet_input = "0"
            force_prefix_pt = True
            ltdbb_variant_choice = st.selectbox("Jenis (Override Opsional)", ["Auto", "G0001", "G0002", "G0003"])
            ltdbb_variant_override = None if ltdbb_variant_choice == "Auto" else ltdbb_variant_choice
            st.caption("Auto akan mencoba deteksi dari nama file, header, dan kolom tujuan.")
    else:
        selected_label = list(MODE_MAP.keys())[0]
        selected_mode = MODE_MAP[selected_label]
        sheet_input = "0"
        force_prefix_pt = True
        ltdbb_variant_override = None
        st.markdown("### Menu Utama")
        st.caption("Pilih tool dari halaman utama di area konten.")

if st.session_state.get("current_view", "home") == "home":
    render_home_page()
else:
    top_action_col1, top_action_col2 = st.columns([1.1, 5])
    with top_action_col1:
        if st.button("← Beranda", use_container_width=True, key="content_go_home"):
            go_home()
    with top_action_col2:
        st.caption("Gunakan tombol ini untuk kembali ke kumpulan tool.")

    if selected_tool == TOOL_LABEL_PK:
        render_hero(
            "📊 Data Cleaning Perlindungan Konsumen",
            "Upload Excel → proses otomatis 3 mode → download hasil",
            "Upload Excel → proses cepat 3 mode → download hasil.",
        )
        render_pk_tool(selected_label, selected_mode, sheet_input, force_prefix_pt)
    else:
        render_hero(
            "🧹 Cleaner Template LTDBB",
            "Upload CSV/XLS/XLSX → cleaning template → ringkasan → export hasil",
            "Fitur baru untuk konversi template LTDBB agar siap dianalisis dan diunduh.",
        )
        render_ltdbb_tool(ltdbb_variant_override)
