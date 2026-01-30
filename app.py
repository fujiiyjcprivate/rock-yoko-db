import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from datetime import datetime

# ==========================================
# ⚙️ 設定・定数定義
# ==========================================
CONFIG = {
    # ↓↓ Secretsから読むのでここはローカル開発用、そのままでOK
    "KEY_FILE": 'secret_key.json', 
    "SHEET_NAME": 'rock_yoko',
    "ADMIN_PASSWORD": "rock", 
    "EVENT_TYPES": [
        "春コン", "新歓", "七夕祭", "サマコン", 
        "外ステ", "11月ライブ", "クリコン", "バレコン", "追いコン", "その他"
    ],
    "PARTS": [
        "Vo", "Gt", "Ba", "Dr", "Key", 
        "GtVo", "BaVo", "KeyVo", "Other"
    ],
    "CIRCLES": ["", "軽音楽部", "フォークソング研究会"],
    "ROLES": ["", "部長", "会計", "PA", "ドラ管", "照明"]
}

# ==========================================
# 🛠️ データベース管理クラス (Model)
# ==========================================
class SheetManager:
    def __init__(self):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # クラウド対応
        if "gcp_service_account" in st.secrets:
            key_dict = st.secrets["gcp_service_account"]
            self.creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, self.scope)
        else:
            self.creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["KEY_FILE"], self.scope)
            
        self.client = gspread.authorize(self.creds)

    @st.cache_resource
    def get_workbook(_self):
        return _self.client.open(CONFIG["SHEET_NAME"])

    def _bool_to_str(self, val):
        return "TRUE" if val else "FALSE"

    def _str_to_bool(self, val):
        if isinstance(val, bool): return val
        return str(val).upper() == "TRUE"

    def get_next_id(self, sheet_name):
        ws = self.get_workbook().worksheet(sheet_name)
        ids = ws.col_values(1)[1:] 
        valid_ids = [int(i) for i in ids if str(i).isdigit()]
        return max(valid_ids) + 1 if valid_ids else 1

    def add_row(self, sheet_name, data_dict):
        ws = self.get_workbook().worksheet(sheet_name)
        new_id = self.get_next_id(sheet_name)
        data_dict['id'] = new_id
        
        header = ws.row_values(1)
        row_values = []
        for h in header:
            val = data_dict.get(h, "")
            if isinstance(val, bool):
                val = self._bool_to_str(val)
            row_values.append(val)
            
        ws.append_row(row_values)
        self.clear_cache()
        return new_id

    def update_row(self, sheet_name, target_id, update_dict):
        ws = self.get_workbook().worksheet(sheet_name)
        cell = ws.find(str(target_id), in_column=1)
        if not cell: return False
        
        header = ws.row_values(1)
        row_num = cell.row
        
        for key, val in update_dict.items():
            if key in header:
                col_num = header.index(key) + 1
                if isinstance(val, bool):
                    val = self._bool_to_str(val)
                ws.update_cell(row_num, col_num, val)
                time.sleep(0.5)
        
        self.clear_cache()
        return True

    def delete_row(self, sheet_name, target_id):
        ws = self.get_workbook().worksheet(sheet_name)
        cell = ws.find(str(target_id), in_column=1)
        if cell:
            ws.delete_rows(cell.row)
            self.clear_cache()
            return True
        return False

    def bulk_insert_performances(self, rows_list):
        if not rows_list: return
        ws = self.get_workbook().worksheet("performances")
        start_id = self.get_next_id("performances")
        header = ws.row_values(1)
        data = []
        for i, r in enumerate(rows_list):
            r['id'] = start_id + i
            data.append([r.get(h, "") for h in header])
        ws.append_rows(data)
        self.clear_cache()

    def clear_cache(self):
        st.cache_data.clear()

    @st.cache_data(ttl=60)
    def load_all_data(_self):
        try:
            wb = _self.get_workbook()
            time.sleep(1)
            
            raw_mem = wb.worksheet("members").get_all_records(numericise_ignore=['all'])
            raw_band = wb.worksheet("bands").get_all_records(numericise_ignore=['all'])
            raw_perf = wb.worksheet("performances").get_all_records(numericise_ignore=['all'])
            
            df_mem = pd.DataFrame(raw_mem)
            df_band = pd.DataFrame(raw_band)
            df_perf = pd.DataFrame(raw_perf)

            def clean_df(df):
                if df.empty: return df
                for col in ['id', 'year', 'band_id', 'member_id']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                if 'is_uso' in df.columns:
                    df['is_uso'] = df['is_uso'].apply(lambda x: _self._str_to_bool(x))
                elif 'is_uso' not in df.columns and not df.empty:
                    df['is_uso'] = False
                return df

            return clean_df(df_mem), clean_df(df_band), clean_df(df_perf)

        except Exception as e:
            st.error(f"データ読み込みエラー: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

db = SheetManager()

# ==========================================
# 🎨 UIコンポーネント (Mobile Optimized)
# ==========================================
def format_year(year_int):
    if year_int == 0: return "全年度"
    try:
        s = str(int(year_int))
        return s[-2:] if len(s) >= 2 else s
    except:
        return "00"

# --- 📱 カード型リスト表示 ---
def render_band_cards(grouped_df):
    """データフレームをスマホで見やすいカード形式で表示"""
    if grouped_df.empty:
        st.warning("条件に一致するバンドはありません")
        return

    # スタイルの調整
    st.markdown("""
    <style>
    .band-card {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 15px;
        border-left: 5px solid #ff4b4b;
    }
    .dark-mode .band-card {
        background-color: #262730;
    }
    .band-title { font-weight: bold; font-size: 1.1em; color: #31333F; }
    .song-title { color: #555; font-style: italic; }
    .event-tag { 
        background-color: #ff4b4b; color: white; 
        padding: 2px 8px; border-radius: 4px; font-size: 0.8em;
    }
    </style>
    """, unsafe_allow_html=True)

    for _, row in grouped_df.iterrows():
        # Streamlitのコンテナ機能を使って枠を作る
        with st.container(border=True):
            # 1行目：アーティスト - 曲名
            st.markdown(f"### **{row['artist_name']}** / {row['song_name']}")
            
            # 2行目：イベント情報
            yr = format_year(row['year_b'])
            ev = row['event_type']
            st.caption(f"📅 {yr}年度 {ev}")
            
            # 3行目：メンバー
            st.write(f"👥 {row['mem_disp']}")
            
            # 4行目：コメント（あれば）
            if row['description']:
                with st.expander("💬 コメントを見る"):
                    st.write(row['description'])

# --- 📱 登録フォーム ---
def render_register_tab(df_mem):
    st.subheader("📝 新規登録")
    
    # 登録対象の切り替え
    reg_type = st.selectbox("登録するもの", ["バンド登録", "部員登録"])
    
    st.divider()

    if reg_type == "バンド登録":
        # 1. 必須情報
        is_uso = st.checkbox("嘘バンとして登録", key="reg_b_uso")
        
        c1, c2 = st.columns(2)
        r_year = c1.number_input("年度", value=datetime.now().year, key="reg_b_y")
        r_event = c2.selectbox("イベント", CONFIG["EVENT_TYPES"], key="reg_b_e")
        
        r_artist = st.text_input("アーティスト名 (必須)", key="reg_b_a")
        r_song = st.text_input("曲名", key="reg_b_s")
        r_desc = st.text_area("コメント", height=80, key="reg_b_d")
        
        st.info("▼ メンバーを追加")
        if 'temp_mems' not in st.session_state: st.session_state.temp_mems = []

        if not df_mem.empty:
            df_mem['opt_label'] = df_mem.apply(lambda x: f"{format_year(x['year'])}{x['name']}", axis=1)
            mem_dict = dict(zip(df_mem['opt_label'], df_mem['id']))
            default_parts = dict(zip(df_mem['id'], df_mem['part']))

            # スマホ向けに縦並びにする
            sel_label = st.selectbox("部員検索", list(mem_dict.keys()), key="reg_sb_mem")
            
            sel_id = mem_dict[sel_label] if sel_label else 0
            def_p = default_parts.get(sel_id, "Gt")
            try: p_idx = CONFIG["PARTS"].index(def_p)
            except: p_idx = 0
            sel_part = st.selectbox("パート", CONFIG["PARTS"], index=p_idx, key="reg_sb_part")

            if st.button("メンバーリストに追加 ➕", use_container_width=True):
                current_ids = [m['id'] for m in st.session_state.temp_mems]
                if sel_id in current_ids:
                    st.error("既に追加されています")
                else:
                    name_only = sel_label
                    st.session_state.temp_mems.append({"id": sel_id, "name": name_only, "part": sel_part})

            # 追加されたメンバー表示
            if st.session_state.temp_mems:
                st.markdown("---")
                st.write("Current Members:")
                for i, m in enumerate(st.session_state.temp_mems):
                    st.text(f"・{m['name']} ({m['part']})")
                
                if st.button("クリア", key="clear_list"):
                    st.session_state.temp_mems = []
                    st.rerun()

                st.markdown("---")
                if st.button("✅ バンドを保存する", type="primary", use_container_width=True):
                    if not r_artist:
                        st.error("アーティスト名は必須です")
                    else:
                        with st.spinner("保存中..."):
                            bid = db.add_row("bands", {
                                "year": r_year, "event_type": r_event, "band_name": "",
                                "artist_name": r_artist, "song_name": r_song, "description": r_desc,
                                "is_uso": is_uso
                            })
                            perfs = [{"band_id": bid, "member_id": m['id'], "part": m['part']} for m in st.session_state.temp_mems]
                            db.bulk_insert_performances(perfs)
                        st.success(f"保存しました！")
                        st.session_state.temp_mems = []
                        time.sleep(1)
                        st.rerun()

    else: # 部員登録
        is_uso = st.checkbox("嘘の部員", key="reg_m_uso")
        name = st.text_input("名前")
        year = st.number_input("年度 (西暦4桁)", value=datetime.now().year)
        
        part = st.selectbox("Main Part", CONFIG["PARTS"])
        sub = st.multiselect("Sub Parts", CONFIG["PARTS"])
        
        circle = st.selectbox("所属", CONFIG["CIRCLES"])
        role = st.selectbox("役職", CONFIG["ROLES"])

        if st.button("部員を保存", type="primary", use_container_width=True):
            if not name:
                st.error("名前を入力してください")
            else:
                with st.spinner("保存中..."):
                    db.add_row("members", {
                        "name": name, "year": year, "part": part, 
                        "sub_parts": ",".join(sub), "circle": circle, "role": role,
                        "is_uso": is_uso
                    })
                st.success(f"登録しました: {name}")
                time.sleep(1)
                st.rerun()

# --- 📱 管理・修正フォーム ---
def render_admin_tab(df_mem, df_band):
    st.subheader("🔧 管理者メニュー")
    password = st.text_input("合言葉 (パスワード)", type="password")
    
    if password != CONFIG["ADMIN_PASSWORD"]:
        if password: st.error("合言葉が違います")
        return

    st.success("認証成功")
    target = st.selectbox("修正対象", ["バンド修正", "部員修正"])

    if target == "部員修正":
        if df_mem.empty: return
        df_mem_sort = df_mem.sort_values(['year', 'id'], ascending=False)
        opts = {f"{format_year(r['year'])} {r['name']}": r for _, r in df_mem_sort.iterrows()}
        
        sel_key = st.selectbox("修正する部員を選択", list(opts.keys()))
        if sel_key:
            tgt = opts[sel_key]
            suffix = f"_{tgt['id']}"
            
            with st.form(f"edit_mem_{suffix}"):
                is_uso = st.checkbox("嘘フラグ", value=tgt.get('is_uso', False))
                name = st.text_input("名前", value=tgt['name'])
                year = st.number_input("年度", value=tgt['year'])
                part = st.selectbox("Main", CONFIG["PARTS"], index=CONFIG["PARTS"].index(tgt['part']) if tgt['part'] in CONFIG["PARTS"] else 0)
                
                defs = [x for x in str(tgt['sub_parts']).split(',') if x in CONFIG["PARTS"]]
                sub = st.multiselect("Sub", CONFIG["PARTS"], default=defs)
                
                c_idx = CONFIG["CIRCLES"].index(tgt.get('circle', '')) if tgt.get('circle') in CONFIG["CIRCLES"] else 0
                circle = st.selectbox("所属", CONFIG["CIRCLES"], index=c_idx)
                
                up_btn = st.form_submit_button("更新する", type="primary")
                
                if up_btn:
                    db.update_row("members", tgt['id'], {
                        "name": name, "year": year, "part": part, 
                        "sub_parts": ",".join(sub), "circle": circle, 
                        "is_uso": is_uso
                    })
                    st.success("更新しました")
                    time.sleep(1)
                    st.rerun()
            
            if st.button("この部員を削除", key=f"del_m_{suffix}"):
                db.delete_row("members", tgt['id'])
                st.warning("削除しました")
                time.sleep(1)
                st.rerun()

    else: # バンド修正
        if df_band.empty: return
        # 検索しやすいようにリスト化
        b_map = {}
        for _, r in df_band.iterrows():
            label = f"[{format_year(r['year'])}{r['event_type']}] {r['artist_name']} / {r['song_name']}"
            b_map[label] = r
            
        sel_bk = st.selectbox("修正するバンドを選択", list(b_map.keys()))
        if sel_bk:
            btgt = b_map[sel_bk]
            suffix = f"_{btgt['id']}"
            
            with st.form(f"edit_band_{suffix}"):
                is_uso = st.checkbox("嘘フラグ", value=btgt.get('is_uso', False))
                art = st.text_input("アーティスト", value=btgt['artist_name'])
                song = st.text_input("曲名", value=btgt['song_name'])
                desc = st.text_area("コメント", value=btgt.get('description', ''))
                
                up_btn = st.form_submit_button("更新する", type="primary")
                
                if up_btn:
                    db.update_row("bands", btgt['id'], {
                        "artist_name": art, "song_name": song, "description": desc,
                        "is_uso": is_uso
                    })
                    st.success("更新しました")
                    time.sleep(1)
                    st.rerun()

            if st.button("このバンドを削除", key=f"del_b_{suffix}"):
                db.delete_row("bands", btgt['id'])
                st.warning("削除しました")
                time.sleep(1)
                st.rerun()

# ==========================================
# 🚀 メイン処理 (Layout)
# ==========================================
def main():
    st.set_page_config(page_title="ロック研DB", layout="centered", initial_sidebar_state="collapsed")
    
    st.markdown("### 🎸 ロック研データベース")
    
    # データロード
    df_mem, df_band, df_perf = db.load_all_data()

    # データ結合処理
    df_full = pd.DataFrame()
    if not df_band.empty and not df_perf.empty and not df_mem.empty:
        mem_ren = df_mem.rename(columns={'year':'year_m', 'name':'name_m', 'part':'part_m', 'sub_parts':'sub_parts_m', 'is_uso':'is_uso_m'})
        band_ren = df_band.rename(columns={'year':'year_b', 'id':'band_id_key', 'is_uso':'is_uso_b'})
        
        df_full = pd.merge(df_perf, mem_ren, left_on='member_id', right_on='id', how='left')
        df_full = pd.merge(df_full, band_ren, left_on='band_id', right_on='band_id_key', how='left')
        
        # 欠損埋め
        df_full.fillna({"name_m": "不明", "part": "?", "artist_name": "不明", "song_name": "不明", "description": ""}, inplace=True)
        # 表示名
        df_full['year_str'] = df_full['year_m'].fillna(0).astype(int).apply(format_year)
        df_full['mem_disp'] = df_full['year_str'].astype(str) + df_full['name_m'].astype(str) + "(" + df_full['part'].astype(str) + ")"

    # --- 📱 タブ構成に変更 ---
    tab_list, tab_reg, tab_admin = st.tabs(["🎵 リスト", "📝 登録", "🔧 管理"])

    # -----------------------
    # 1. リストタブ
    # -----------------------
    with tab_list:
        # 検索フィルターはアコーディオンに隠す
        with st.expander("🔍 検索・絞り込み条件"):
            f_uso = st.checkbox("嘘バンも含める", value=False)
            f_kw = st.text_input("キーワード検索", placeholder="曲名・アーティスト・コメント")
            
            c1, c2 = st.columns(2)
            f_year = c1.selectbox("年度", [0] + list(range(2020, 2030)), format_func=lambda x: f"{format_year(x)}年度")
            f_event = c2.selectbox("イベント", ["すべて"] + CONFIG["EVENT_TYPES"])
            
            c3, c4 = st.columns(2)
            f_part = c3.selectbox("パート", ["すべて"] + CONFIG["PARTS"])
            f_circle = c4.selectbox("所属", ["すべて"] + CONFIG["CIRCLES"])

        if df_full.empty:
            st.info("データがありません")
        else:
            view_df = df_full.copy()
            # フィルタリング
            if not f_uso:
                if 'is_uso_b' in view_df.columns: view_df = view_df[view_df['is_uso_b'] != True]
            if f_year > 0: view_df = view_df[view_df['year_b'] == f_year]
            if f_event != "すべて": view_df = view_df[view_df['event_type'] == f_event]
            if f_kw:
                mask = view_df[['artist_name', 'song_name', 'description']].astype(str).apply(lambda x: x.str.contains(f_kw, na=False)).any(axis=1)
                view_df = view_df[mask]
            
            # 部員絞り込み
            if f_part != "すべて":
                t_ids = view_df[
                    (view_df['part_m'] == f_part) | 
                    (view_df['sub_parts_m'].astype(str).str.contains(f_part, na=False)) |
                    (view_df['part'] == f_part)
                ]['band_id'].unique()
                view_df = view_df[view_df['band_id'].isin(t_ids)]
            if f_circle != "すべて":
                t_ids_c = view_df[view_df['circle'] == (f_circle if f_circle else "")]['band_id'].unique()
                view_df = view_df[view_df['band_id'].isin(t_ids_c)]

            # グルーピングして表示
            if not view_df.empty:
                grouped = view_df.groupby(['band_id', 'year_b', 'event_type', 'artist_name', 'song_name', 'description'])['mem_disp'].apply(lambda x: ", ".join(x.astype(str))).reset_index()
                grouped = grouped.sort_values(['year_b', 'band_id'], ascending=[False, False])
                
                st.caption(f"{len(grouped)}件のバンドが見つかりました")
                render_band_cards(grouped)
            else:
                st.warning("条件に一致するバンドはありません")

    # -----------------------
    # 2. 登録タブ
    # -----------------------
    with tab_reg:
        render_register_tab(df_mem)

    # -----------------------
    # 3. 管理タブ
    # -----------------------
    with tab_admin:
        render_admin_tab(df_mem, df_band)
        
        # 要望フォームもここに移動
        st.divider()
        with st.expander("📢 要望・バグ報告"):
            with st.form("report_form"):
                rep_msg = st.text_area("内容")
                if st.form_submit_button("送信"):
                    if rep_msg:
                        try:
                            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            db.add_row("reports", {"timestamp": ts, "message": rep_msg})
                            st.success("送信しました")
                        except: st.error("送信エラー")

if __name__ == "__main__":
    main()
