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
    "KEY_FILE": 'secret_key.json',
    "SHEET_NAME": 'rock_yoko',
    "ADMIN_PASSWORD": "rock",  # 修正・削除のための合言葉
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
        
        # --- クラウド対応の分岐処理 ---
        # 1. Streamlit CloudのSecretsに鍵がある場合 (デプロイ環境)
        if "gcp_service_account" in st.secrets:
            # Secretsから辞書データとして読み込む
            key_dict = st.secrets["gcp_service_account"]
            self.creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, self.scope)
        # 2. ローカルにJSONファイルがある場合 (開発環境)
        else:
            self.creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["KEY_FILE"], self.scope)
            
        self.client = gspread.authorize(self.creds)

    @st.cache_resource
    def get_workbook(_self):
        return _self.client.open(CONFIG["SHEET_NAME"])

    def _bool_to_str(self, val):
        """Pythonのboolをスプシ用の文字列に変換"""
        return "TRUE" if val else "FALSE"

    def _str_to_bool(self, val):
        """スプシの文字列をPythonのboolに変換"""
        if isinstance(val, bool): return val
        return str(val).upper() == "TRUE"

    def get_next_id(self, sheet_name):
        ws = self.get_workbook().worksheet(sheet_name)
        ids = ws.col_values(1)[1:] # ヘッダー除外
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
                time.sleep(0.5) # API制限対策
        
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
        """全データを取得してDataFrame化・型変換を行う"""
        try:
            wb = _self.get_workbook()
            time.sleep(1) # API制限対策
            
            # 各シート取得
            raw_mem = wb.worksheet("members").get_all_records(numericise_ignore=['all'])
            raw_band = wb.worksheet("bands").get_all_records(numericise_ignore=['all'])
            raw_perf = wb.worksheet("performances").get_all_records(numericise_ignore=['all'])
            
            df_mem = pd.DataFrame(raw_mem)
            df_band = pd.DataFrame(raw_band)
            df_perf = pd.DataFrame(raw_perf)

            # 型変換ユーティリティ
            def clean_df(df):
                if df.empty: return df
                # ID系は数値へ
                for col in ['id', 'year', 'band_id', 'member_id']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                # 嘘フラグはBoolへ
                if 'is_uso' in df.columns:
                    df['is_uso'] = df['is_uso'].apply(lambda x: _self._str_to_bool(x))
                elif 'is_uso' not in df.columns and not df.empty:
                    # カラム不足時のフォールバック
                    df['is_uso'] = False
                return df

            return clean_df(df_mem), clean_df(df_band), clean_df(df_perf)

        except Exception as e:
            st.error(f"データ読み込みエラー: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# インスタンス化
db = SheetManager()

# ==========================================
# 🎨 UIコンポーネント関数 (View)
# ==========================================
def render_header():
    st.markdown("### 都立大ロック研データベース")
    st.write("ロック研の部員や、過去のライブで演奏されたバンドを検索できます。部員登録していない人がいる場合はバンド登録ができません。画面左の部分では\"検索\"ができます。右側ではバンドと部員の\"追加\"ができます。")

def format_year(year_int):
    """西暦4桁(2024) -> 文字列2桁(24)"""
    if year_int == 0: return "全年度"
    try:
        s = str(int(year_int))
        return s[-2:] if len(s) >= 2 else s
    except:
        return "00"

def render_search_column(left_col):
    """左カラム：検索フィルター"""
    with left_col:
        st.subheader("🔍 検索")
        
        # 1. 嘘フィルター (最優先)
        show_uso = st.checkbox("嘘の", value=False)
        st.divider()

        # 2. バンド検索
        st.markdown("##### 🎸 バンド")
        s_year = st.selectbox("年度", [0] + list(range(2020, 2030)), format_func=lambda x: f"{format_year(x)}年度")
        s_event = st.selectbox("イベント", ["すべて"] + CONFIG["EVENT_TYPES"])
        s_keyword = st.text_input("キーワード", placeholder="曲名・アーティスト・コメント")
        
        st.divider()

        # 3. 部員検索
        st.markdown("##### 👤 部員")
        s_part = st.selectbox("パート", ["すべて"] + CONFIG["PARTS"])
        s_circle = st.selectbox("所属", ["すべて"] + CONFIG["CIRCLES"])
        
        return show_uso, s_year, s_event, s_keyword, s_part, s_circle

def render_action_column(right_col, df_mem, df_band):
    """右カラム：登録・編集"""
    with right_col:
        # 1. 登録モード（デフォルト）
        render_register_mode(df_mem)

        st.divider()

        # 2. 管理者メニュー（修正・削除）
        # パスワードで保護する
        with st.expander("🔧 管理者メニュー (修正・削除)"):
            st.caption("合言葉を入力すると修正画面が開きます")
            password = st.text_input("合言葉", type="password", key="admin_pass")
            
            if password == CONFIG["ADMIN_PASSWORD"]:
                st.success("認証しました")
                render_edit_mode(df_mem, df_band)
            elif password:
                st.error("合言葉が違います")

def render_register_mode(df_mem):
    """登録モードのUI"""
    st.subheader("📝 追加")
    target = st.radio("対象", ["バンド登録", "部員登録"], horizontal=True)

    if target == "バンド登録":
        # 嘘チェック (最初)
        is_uso = st.checkbox("嘘のバンド", key="reg_b_uso")
        
        # 必須情報
        if 'temp_mems' not in st.session_state: st.session_state.temp_mems = []
        
        col1, col2 = st.columns(2)
        r_year = col1.number_input("年度 (西暦4桁)", value=datetime.now().year, key="reg_b_y")
        r_event = col2.selectbox("イベント", CONFIG["EVENT_TYPES"], key="reg_b_e")
        r_artist = st.text_input("アーティスト (必須)", key="reg_b_a")
        r_song = st.text_input("曲名", key="reg_b_s")
        r_desc = st.text_area("コメント", key="reg_b_d")

        st.markdown("---")
        st.caption("▼ メンバー選択")
        
        # 部員選択プルダウン作成
        if not df_mem.empty:
            df_mem['opt_label'] = df_mem.apply(lambda x: f"{format_year(x['year'])}{x['name']}({x['part']})", axis=1)
            mem_dict = dict(zip(df_mem['opt_label'], df_mem['id']))
            default_parts = dict(zip(df_mem['id'], df_mem['part']))

            c1, c2 = st.columns([2, 1])
            sel_label = c1.selectbox("部員", list(mem_dict.keys()), key="reg_sb_mem")
            
            # デフォルトパートの自動選択
            sel_id = mem_dict[sel_label] if sel_label else 0
            def_p = default_parts.get(sel_id, "Gt")
            try: p_idx = CONFIG["PARTS"].index(def_p)
            except: p_idx = 0
            sel_part = c2.selectbox("パート", CONFIG["PARTS"], index=p_idx, key="reg_sb_part")

            # 追加・クリアボタン
            b1, b2 = st.columns(2)
            if b1.button("リストに追加 ➕"):
                current_ids = [m['id'] for m in st.session_state.temp_mems]
                if sel_id in current_ids:
                    st.error("既に追加されています")
                else:
                    name_only = sel_label.split("(")[0]
                    st.session_state.temp_mems.append({"id": sel_id, "name": name_only, "part": sel_part})
            
            if b2.button("リストをクリア"):
                st.session_state.temp_mems = []

            # 現在のリスト表示
            if st.session_state.temp_mems:
                st.info("参加: " + ", ".join([f"{m['name']}({m['part']})" for m in st.session_state.temp_mems]))
                
                # 保存ボタン
                if st.button("✅ バンドを保存", type="primary"):
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
                        st.success(f"保存しました: {r_artist}")
                        st.session_state.temp_mems = []
                        time.sleep(1)
                        st.rerun()

    else: # 部員登録
        # 嘘チェック (最初)
        is_uso = st.checkbox("嘘の部員", key="reg_m_uso")

        name = st.text_input("名前")
        year = st.number_input("年度 (西暦4桁)", value=datetime.now().year)
        
        c1, c2 = st.columns(2)
        part = c1.selectbox("Main", CONFIG["PARTS"])
        sub = c2.multiselect("Sub", CONFIG["PARTS"])
        
        c3, c4 = st.columns(2)
        circle = c3.selectbox("所属", CONFIG["CIRCLES"])
        role = c4.selectbox("役職", CONFIG["ROLES"])

        if st.button("部員を保存", type="primary"):
            if not name:
                st.error("名前を入力してください")
            else:
                dup = df_mem[(df_mem['name'] == name) & (df_mem['year'] == year)]
                if not dup.empty:
                    st.warning("同姓同名の部員が既にその年度に存在します")
                
                with st.spinner("保存中..."):
                    db.add_row("members", {
                        "name": name, "year": year, "part": part, 
                        "sub_parts": ",".join(sub), "circle": circle, "role": role,
                        "is_uso": is_uso
                    })
                st.success(f"登録しました: {name}")
                time.sleep(1)
                st.rerun()

def render_edit_mode(df_mem, df_band):
    """修正モードのUI (認証後)"""
    target = st.radio("修正対象", ["バンド修正", "部員修正"], horizontal=True)

    if target == "部員修正":
        if df_mem.empty:
            st.write("データがありません")
            return
            
        df_mem_sort = df_mem.sort_values(['year', 'id'], ascending=False)
        opts = {f"{format_year(r['year'])} {r['name']}": r for _, r in df_mem_sort.iterrows()}
        
        sel_key = st.selectbox("修正する部員", list(opts.keys()))
        if sel_key:
            tgt = opts[sel_key]
            
            # 【重要】キーにIDを含めることで、選択切り替え時にリロード（ウィジェットの再描画）を強制する
            suffix = f"_{tgt['id']}"
            
            is_uso = st.checkbox("嘘の部員", value=tgt.get('is_uso', False), key=f"edt_m_uso{suffix}")
            name = st.text_input("名前", value=tgt['name'], key=f"edt_m_n{suffix}")
            year = st.number_input("年度", value=tgt['year'], key=f"edt_m_y{suffix}")
            
            c1, c2 = st.columns(2)
            try: p_idx = CONFIG["PARTS"].index(tgt['part'])
            except: p_idx = 0
            part = c1.selectbox("Main", CONFIG["PARTS"], index=p_idx, key=f"edt_m_p{suffix}")
            
            defs = [x for x in str(tgt['sub_parts']).split(',') if x in CONFIG["PARTS"]]
            sub = c2.multiselect("Sub", CONFIG["PARTS"], default=defs, key=f"edt_m_s{suffix}")
            
            c3, c4 = st.columns(2)
            try: ci_idx = CONFIG["CIRCLES"].index(tgt.get('circle', ''))
            except: ci_idx = 0
            try: ro_idx = CONFIG["ROLES"].index(tgt.get('role', ''))
            except: ro_idx = 0
            
            circle = c3.selectbox("所属", CONFIG["CIRCLES"], index=ci_idx, key=f"edt_m_c{suffix}")
            role = c4.selectbox("役職", CONFIG["ROLES"], index=ro_idx, key=f"edt_m_r{suffix}")

            col_up, col_del = st.columns(2)
            if col_up.button("更新", type="primary", key=f"btn_up_m{suffix}"):
                db.update_row("members", tgt['id'], {
                    "name": name, "year": year, "part": part, 
                    "sub_parts": ",".join(sub), "circle": circle, "role": role, 
                    "is_uso": is_uso
                })
                st.success("更新しました")
                time.sleep(1)
                st.rerun()
                
            if col_del.button("削除", type="secondary", key=f"btn_del_m{suffix}"):
                db.delete_row("members", tgt['id'])
                st.warning("削除しました")
                time.sleep(1)
                st.rerun()

    else: # バンド修正
        if df_band.empty:
            st.write("データがありません")
            return

        b_map = {}
        for _, r in df_band.iterrows():
            disp = r['artist_name'] if r['artist_name'] else "名称未設定"
            label = f"[{format_year(r['year'])}{r['event_type']}] {disp} / {r['song_name']}"
            b_map[label] = r
            
        sel_bk = st.selectbox("修正するバンド", list(b_map.keys()))
        if sel_bk:
            btgt = b_map[sel_bk]
            
            # 【重要】キーにIDを含めてリロードを強制する
            suffix = f"_{btgt['id']}"
            
            is_uso = st.checkbox("嘘のバンド", value=btgt.get('is_uso', False), key=f"edt_b_uso{suffix}")
            art = st.text_input("アーティスト", value=btgt['artist_name'], key=f"edt_b_a{suffix}")
            song = st.text_input("曲名", value=btgt['song_name'], key=f"edt_b_s{suffix}")
            desc = st.text_area("コメント", value=btgt.get('description', ''), key=f"edt_b_d{suffix}")

            col_up, col_del = st.columns(2)
            if col_up.button("更新", type="primary", key=f"btn_up_b{suffix}"):
                db.update_row("bands", btgt['id'], {
                    "artist_name": art, "song_name": song, "description": desc,
                    "is_uso": is_uso
                })
                st.success("更新しました")
                time.sleep(1)
                st.rerun()
                
            if col_del.button("削除", type="secondary", key=f"btn_del_b{suffix}"):
                db.delete_row("bands", btgt['id'])
                st.warning("削除しました")
                time.sleep(1)
                st.rerun()

def render_footer():
    st.divider()
    with st.expander("📢 開発者への要望・バグ報告はこちら"):
        with st.form("report_form"):
            c1, c2 = st.columns([1, 2])
            rep_type = c1.selectbox("種別", ["要望・機能リクエスト", "バグ・不具合報告", "その他"])
            rep_msg = c2.text_area("内容 (詳細に書いてくれると助かります！)")
            
            if st.form_submit_button("送信"):
                if rep_msg:
                    try:
                        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        db.add_row("reports", {
                            "timestamp": ts, "type": rep_type, "message": rep_msg
                        })
                        st.success("報告ありがとうございます！DBに記録しました。")
                    except Exception as e:
                        st.error(f"送信エラー: {e}")
                else:
                    st.error("内容を入力してください")

# ==========================================
# 🚀 メイン処理フロー
# ==========================================
def main():
    st.set_page_config(page_title="ロック研データベース", layout="wide")
    
    # 1. ヘッダー
    render_header()
    
    # 2. データロード
    df_mem, df_band, df_perf = db.load_all_data()

    # 3. データ結合 (表示用)
    df_full = pd.DataFrame()
    if not df_band.empty and not df_perf.empty and not df_mem.empty:
        mem_ren = df_mem.rename(columns={'year':'year_m', 'name':'name_m', 'part':'part_m', 'sub_parts':'sub_parts_m', 'is_uso':'is_uso_m'})
        band_ren = df_band.rename(columns={'year':'year_b', 'id':'band_id_key', 'is_uso':'is_uso_b'})
        
        df_full = pd.merge(df_perf, mem_ren, left_on='member_id', right_on='id', how='left')
        df_full = pd.merge(df_full, band_ren, left_on='band_id', right_on='band_id_key', how='left')
        
        # --- 🛡️ 安全対策: Join失敗による欠損値(NaN)を埋める ---
        df_full['name_m'] = df_full['name_m'].fillna("不明")
        df_full['part'] = df_full['part'].fillna("?")
        df_full['artist_name'] = df_full['artist_name'].fillna("不明")
        df_full['song_name'] = df_full['song_name'].fillna("不明")
        df_full['description'] = df_full['description'].fillna("")

        # 表示用文字列作成
        df_full['year_str'] = df_full['year_m'].fillna(0).astype(int).apply(format_year)
        # すべて強制的に文字列化して結合 (TypeError防止)
        df_full['mem_disp'] = df_full['year_str'].astype(str) + df_full['name_m'].astype(str) + "(" + df_full['part'].astype(str) + ")"

    # 4. カラムレイアウト (左：検索、中：一覧、右：操作)
    col_left, col_center, col_right = st.columns([1.1, 2.8, 1.1])

    # --- 左カラム ---
    filters = render_search_column(col_left)
    f_uso, f_year, f_event, f_kw, f_part, f_circle = filters

    # --- 中カラム (リスト表示) ---
    with col_center:
        st.subheader("🎹 出演リスト")
        
        if df_full.empty:
            st.info("データがありません。右側のフォームから登録してください。")
        else:
            view_df = df_full.copy()

            if not f_uso:
                if 'is_uso_b' in view_df.columns:
                    view_df = view_df[view_df['is_uso_b'] != True]
            
            if f_year > 0: view_df = view_df[view_df['year_b'] == f_year]
            if f_event != "すべて": view_df = view_df[view_df['event_type'] == f_event]
            if f_kw:
                mask = view_df[['artist_name', 'song_name', 'description']].astype(str).apply(lambda x: x.str.contains(f_kw, na=False)).any(axis=1)
                view_df = view_df[mask]
            
            if f_part != "すべて":
                t_ids = view_df[
                    (view_df['part_m'] == f_part) | 
                    (view_df['sub_parts_m'].astype(str).str.contains(f_part, na=False)) |
                    (view_df['part'] == f_part)
                ]['band_id'].unique()
                view_df = view_df[view_df['band_id'].isin(t_ids)]
                
            if f_circle != "すべて":
                target_c = "" if f_circle == "" else f_circle
                t_ids_c = view_df[view_df['circle'] == target_c]['band_id'].unique()
                view_df = view_df[view_df['band_id'].isin(t_ids_c)]

            if not view_df.empty:
                # メンバーを連結 (全て文字列型であることを保証)
                grouped = view_df.groupby(['band_id', 'year_b', 'event_type', 'artist_name', 'song_name', 'description'])['mem_disp'].apply(lambda x: ", ".join(x.astype(str))).reset_index()
                grouped = grouped.sort_values(['year_b', 'band_id'], ascending=[False, False])
                
                grouped['年度'] = grouped['year_b'].apply(format_year)
                
                st.dataframe(
                    grouped.rename(columns={
                        'event_type': 'イベント', 'artist_name': 'アーティスト', 
                        'song_name': '曲名', 'mem_disp': 'メンバー', 'description': 'コメント'
                    })[['年度', 'イベント', 'アーティスト', 'メンバー', '曲名', 'コメント']],
                    use_container_width=True,
                    height=450,
                    hide_index=True
                )
            else:
                st.warning("条件に一致するバンドはありません")

        st.subheader("👤 部員名簿")
        if not df_mem.empty:
            m_view = df_mem.copy()
            
            if not f_uso:
                if 'is_uso' in m_view.columns:
                    m_view = m_view[m_view['is_uso'] != True]
            
            if f_part != "すべて":
                m_view = m_view[(m_view['part'] == f_part) | (m_view['sub_parts'].astype(str).str.contains(f_part, na=False))]
            if f_circle != "すべて":
                target_c = "" if f_circle == "" else f_circle
                m_view = m_view[m_view['circle'] == target_c]

            m_view['入学'] = m_view['year'].apply(format_year)
            m_view['名前'] = m_view['name']
            m_view['Main'] = m_view['part']
            m_view['Sub'] = m_view['sub_parts'] if 'sub_parts' in m_view.columns else ""
            m_view['所属'] = m_view['circle'] if 'circle' in m_view.columns else ""
            m_view['役職'] = m_view['role'] if 'role' in m_view.columns else ""
            
            m_view = m_view.sort_values(['year', 'id'], ascending=[False, True])
            
            st.dataframe(
                m_view[['入学', '名前', 'Main', 'Sub', '所属', '役職']],
                use_container_width=True,
                height=300,
                hide_index=True
            )

    # --- 右カラム (操作) ---
    render_action_column(col_right, df_mem, df_band)

    # 5. フッター
    render_footer()

if __name__ == "__main__":
    main()