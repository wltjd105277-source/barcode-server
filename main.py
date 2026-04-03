from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn
import pandas as pd
import io, os, httpx, json
from urllib.parse import quote
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────
# 이카운트 ERP API 설정 (.env에서 로드)
# ──────────────────────────────────────────────
ECOUNT_CONFIG = {
    "COM_CODE":     os.getenv("ECOUNT_COM_CODE", ""),
    "USER_ID":      os.getenv("ECOUNT_USER_ID", ""),
    "API_CERT_KEY": os.getenv("ECOUNT_API_KEY", ""),
    "ZONE":         os.getenv("ECOUNT_ZONE", "CD"),
    "LAN_TYPE":     "ko-KR",
    "CUST":         os.getenv("ECOUNT_CUST", ""),
    "WH_CD":        os.getenv("ECOUNT_WH_CD", ""),
}


async def get_ecount_session() -> tuple[str, str]:
    """이카운트 세션 ID 발급 (Zone API → OAPILogin 2단계)"""
    async with httpx.AsyncClient(timeout=30) as client:
        # ── 1단계: Zone API로 실제 Zone 조회 ──────────────
        zone_url = "https://oapi.ecount.com/OAPI/V2/Zone"
        zone_resp = await client.post(zone_url, json={"COM_CODE": ECOUNT_CONFIG["COM_CODE"]})
        zone_data = zone_resp.json()
        print(f"📡 Zone API 응답: {zone_data}")
        if str(zone_data.get("Status")) != "200" or not zone_data.get("Data"):
            raise Exception(f"Zone 조회 실패: {zone_data}")
        zone = zone_data["Data"]["ZONE"]          # 예: "CD"
        print(f"✅ Zone 확인: {zone}")

        # ── 2단계: OAPILogin으로 세션 발급 ────────────────
        login_url = f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/OAPILogin"
        login_payload = {
            "COM_CODE":     ECOUNT_CONFIG["COM_CODE"],
            "USER_ID":      ECOUNT_CONFIG["USER_ID"].upper(),
            "API_CERT_KEY": ECOUNT_CONFIG["API_CERT_KEY"],
            "LAN_TYPE":     ECOUNT_CONFIG["LAN_TYPE"],
            "ZONE":         zone,
        }
        login_resp = await client.post(login_url, json=login_payload)
        login_data = login_resp.json()
        print(f"🔑 Login API 응답: {login_data}")

        # 오류 체크 (Status=200이지만 Error가 있을 수 있음)
        if login_data.get("Error"):
            raise Exception(f"이카운트 로그인 실패: {login_data['Error']}")
        if str(login_data.get("Status")) != "200":
            raise Exception(f"이카운트 로그인 실패: {login_data}")

        session_id = login_data["Data"]["Datas"]["SESSION_ID"]
        print(f"✅ 세션 발급 성공: {session_id[:10]}...")
        return session_id, zone

app = FastAPI()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

MASTER_PATH = os.path.join(BASE_DIR, "master_data.xlsx")


# ──────────────────────────────────────────────
# 마스터 데이터 로드 헬퍼
# ──────────────────────────────────────────────
def load_master():
    """master_data.xlsx에서 매핑 및 단종/매입가인상 목록을 로드한다."""
    barcode_to_code = {}  # PO용: 바코드 → 상품코드
    code_to_barcode = {}  # 주문서용: 품목코드 → lineup11 바코드
    discontinued    = set()  # 단종: 바코드 or 품목코드
    price_up        = set()  # 매입가인상: 바코드 or 품목코드

    if not os.path.exists(MASTER_PATH):
        print(f"❌ master_data.xlsx 없음: {MASTER_PATH}")
        return barcode_to_code, code_to_barcode, discontinued, price_up

    # 파일을 메모리에 한 번에 읽고 즉시 닫아 Windows 파일 잠금 방지
    with open(MASTER_PATH, "rb") as f:
        raw = io.BytesIO(f.read())

    xl = pd.ExcelFile(raw)

    # PO 매핑
    po_sheet = "PO매핑" if "PO매핑" in xl.sheet_names else xl.sheet_names[0]
    raw.seek(0)
    df_po = pd.read_excel(raw, sheet_name=po_sheet, dtype=str)
    df_po.columns = df_po.columns.str.strip()
    if "바코드" in df_po.columns and "상품코드" in df_po.columns:
        df_po["바코드"]   = df_po["바코드"].fillna("").str.strip().str.replace(r"\.0$", "", regex=True)
        df_po["상품코드"] = df_po["상품코드"].fillna("").str.strip()
        barcode_to_code = {
            row["바코드"]: row["상품코드"]
            for _, row in df_po.iterrows()
            if row["바코드"]
        }
        print(f"✅ PO 매핑 로드: {len(barcode_to_code)}개")

    # 주문서 매핑
    if "주문서매핑" in xl.sheet_names:
        raw.seek(0)
        df_ord = pd.read_excel(raw, sheet_name="주문서매핑", dtype=str)
        df_ord.columns = df_ord.columns.str.strip()
        if "품목코드" in df_ord.columns and "lineup11 바코드" in df_ord.columns:
            df_ord["품목코드"]        = df_ord["품목코드"].fillna("").str.strip()
            df_ord["lineup11 바코드"] = df_ord["lineup11 바코드"].fillna("").str.strip().str.replace(r"\.0$", "", regex=True)
            code_to_barcode = {
                row["품목코드"]: row["lineup11 바코드"]
                for _, row in df_ord.iterrows()
                if row["품목코드"]
            }
            with_bc = sum(1 for v in code_to_barcode.values() if v)
            print(f"✅ 주문서 매핑 로드: {len(code_to_barcode)}개 (바코드 있음: {with_bc}개)")

    # 단종 목록 (바코드 or 품목코드)
    if "단종" in xl.sheet_names:
        raw.seek(0)
        df_d = pd.read_excel(raw, sheet_name="단종", dtype=str).fillna("")
        df_d.columns = df_d.columns.str.strip()
        for col in ["바코드", "품목코드"]:
            if col in df_d.columns:
                vals = df_d[col].str.strip().str.replace(r"\.0$", "", regex=True)
                discontinued |= set(v for v in vals if v)
        print(f"✅ 단종 목록: {len(discontinued)}개")

    # 매입가인상 목록
    if "매입가인상" in xl.sheet_names:
        raw.seek(0)
        df_p = pd.read_excel(raw, sheet_name="매입가인상", dtype=str).fillna("")
        df_p.columns = df_p.columns.str.strip()
        for col in ["바코드", "품목코드"]:
            if col in df_p.columns:
                vals = df_p[col].str.strip().str.replace(r"\.0$", "", regex=True)
                price_up |= set(v for v in vals if v)
        print(f"✅ 매입가인상 목록: {len(price_up)}개")

    return barcode_to_code, code_to_barcode, discontinued, price_up


# ──────────────────────────────────────────────
# 라우트
# ──────────────────────────────────────────────
@app.get("/")
async def home(request: Request):
    return templates.TemplateResponse(
        request=request, name="index.html", context={"request": request}
    )


@app.get("/api/master-info")
async def master_info():
    """마스터 데이터 현황 반환"""
    barcode_to_code, code_to_barcode, discontinued, price_up = load_master()
    return {
        "po_count":           len(barcode_to_code),
        "order_count":        len(code_to_barcode),
        "order_with_barcode": sum(1 for v in code_to_barcode.values() if v),
        "discontinued_count": len(discontinued),
        "price_up_count":     len(price_up),
    }


# ── PO 파일 처리 ──────────────────────────────
@app.post("/api/process-po")
async def process_po(
    file:       UploadFile = File(...),
    barcodeCol: str        = Form("상품바코드"),
    codeCol:    str        = Form("상품코드"),
):
    print(f"\n--- 🚀 PO [{file.filename}] 시작 ---")
    barcode_to_code, _, _d, _p = load_master()

    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents), dtype=str)
    df.columns = df.columns.str.strip()

    target_bc = barcodeCol.strip()
    if target_bc not in df.columns:
        print(f"❌ '{target_bc}' 열 없음. 현재 열: {list(df.columns)}")
        return JSONResponse(status_code=400, content={"detail": f"'{target_bc}' 열이 없습니다."})

    df[target_bc] = df[target_bc].fillna("").str.strip().str.replace(r"\.0$", "", regex=True)
    codes = df[target_bc].map(lambda bc: barcode_to_code.get(bc, ""))

    if codeCol in df.columns:
        df[codeCol] = codes
    else:
        idx = df.columns.get_loc(target_bc) + 1
        df.insert(idx, codeCol, codes)

    total   = int((df[target_bc] != "").sum())
    matched = int(((df[target_bc] != "") & (df[codeCol] != "")).sum())
    missed  = total - matched
    print(f"📊 PO 결과 - 전체:{total} 성공:{matched} 실패:{missed}")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)

    safe_name = quote(file.filename.replace(".xlsx", "_상품코드.xlsx").replace(".xls", "_상품코드.xlsx"))
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition":          f"attachment; filename*=UTF-8''{safe_name}",
            "X-Total":                      str(total),
            "X-Matched":                    str(matched),
            "X-Missed":                     str(missed),
            "Access-Control-Expose-Headers": "X-Total, X-Matched, X-Missed",
        },
    )


# ── 주문서 처리 ───────────────────────────────
@app.post("/api/process-order")
async def process_order(
    file:       UploadFile = File(...),
    codeCol:    str        = Form("품목코드"),
    barcodeCol: str        = Form("lineup11 바코드"),
    headerRow:  int        = Form(1),
):
    print(f"\n--- 🚀 주문서 [{file.filename}] 시작 ---")
    _, code_to_barcode, _d, _p = load_master()

    contents  = await file.read()
    header_idx = headerRow - 1
    df = pd.read_excel(io.BytesIO(contents), header=header_idx, dtype=str)
    df.columns = df.columns.str.strip()

    target_code = codeCol.strip()
    target_bc   = barcodeCol.strip()

    if target_code not in df.columns:
        print(f"❌ '{target_code}' 열 없음. 현재 열: {list(df.columns)}")
        return JSONResponse(status_code=400, content={"detail": f"'{target_code}' 열이 없습니다."})

    df[target_code] = df[target_code].fillna("").str.strip()
    barcodes = df[target_code].map(lambda c: code_to_barcode.get(c, ""))

    if target_bc in df.columns:
        df[target_bc] = barcodes
    else:
        idx = df.columns.get_loc(target_code) + 1
        df.insert(idx, target_bc, barcodes)

    total   = int((df[target_code] != "").sum())
    matched = int(((df[target_code] != "") & (df[target_bc] != "")).sum())
    missed  = total - matched
    print(f"📊 주문서 결과 - 전체:{total} 성공:{matched} 실패:{missed}")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)

    safe_name = quote(file.filename.replace(".xlsx", "_바코드.xlsx").replace(".xls", "_바코드.xlsx"))
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition":          f"attachment; filename*=UTF-8''{safe_name}",
            "X-Total":                      str(total),
            "X-Matched":                    str(matched),
            "X-Missed":                     str(missed),
            "Access-Control-Expose-Headers": "X-Total, X-Matched, X-Missed",
        },
    )


# ── PO 파일 미리보기 ──────────────────────────
@app.post("/api/parse-po")
async def parse_po(file: UploadFile = File(...)):
    """PO 파일을 읽어 항목 목록 반환 (납품부족사유 선택용)"""
    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents), dtype=str).fillna("")
    df.columns = df.columns.str.strip()

    barcode_to_code, _, discontinued, price_up = load_master()

    items = []
    for i, row in df.iterrows():
        bc = str(row.get("상품바코드", "")).strip().replace(".0", "")
        prod_cd = barcode_to_code.get(bc, "")

        # 파일에 이미 기입된 사유 우선, 없으면 마스터 자동 감지
        existing_reason = str(row.get("납품부족사유", "")).strip()
        if existing_reason:
            auto_reason = existing_reason
        elif bc in discontinued or (prod_cd and prod_cd in discontinued):
            auto_reason = "제품 단종 - 제조사 생산중단 혹은 공급사 취급중단 - 시장 단종"
        elif bc in price_up or (prod_cd and prod_cd in price_up):
            auto_reason = "제품 인상 - 가격 이슈 (Price) - 매입가 인상 협상 중"
        else:
            auto_reason = ""

        items.append({
            "idx":       i,
            "발주번호":  str(row.get("발주번호", "")).strip(),
            "물류센터":  str(row.get("물류센터", "")).strip(),
            "상품이름":  str(row.get("상품이름", "")).strip()[:40],
            "발주수량":  str(row.get("발주수량", "")).strip(),
            "확정수량":  str(row.get("확정수량", "")).strip(),
            "매핑여부":  "✅" if prod_cd else "❌",
            "사유":      auto_reason,
        })
    return JSONResponse(content={"items": items, "total": len(items)})


# ── PO 파일 다운로드 (납품부족사유 반영) ──────────
@app.post("/api/download-po")
async def download_po(
    file:             UploadFile = File(...),
    shortage_reasons: str        = Form("{}"),
):
    """납품부족사유를 채워서 PO 파일 다운로드"""
    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents), dtype=str).fillna("")
    df.columns = df.columns.str.strip()

    reasons: dict = json.loads(shortage_reasons)
    for idx_str, reason in reasons.items():
        idx = int(idx_str)
        if idx < len(df):
            df.at[idx, "납품부족사유"] = reason

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)

    safe_name = quote(file.filename.replace(".xlsx", "_납품부족사유.xlsx"))
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{safe_name}"},
    )


# ── 이카운트 판매 전표 등록 ─────────────────────
# PO_ 파일을 업로드하면 자동으로 변환 후 이카운트에 전송
@app.post("/api/send-to-ecount")
async def send_to_ecount(
    file:             UploadFile = File(...),
    staff_code:       str        = Form(""),   # 담당자 코드
    io_date:          str        = Form(""),   # 판매일자 (yyyymmdd), 비어있으면 오늘
    shortage_reasons: str        = Form("{}"), # {idx: 사유} JSON
):
    print(f"\n--- 📤 이카운트 전송 [{file.filename}] 시작 ---")
    today = io_date.strip() if io_date.strip() else datetime.today().strftime("%Y%m%d")
    print(f"📅 판매일자: {today}")
    barcode_to_code, _, discontinued, price_up = load_master()

    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), dtype=str)
    except Exception as e:
        return JSONResponse(status_code=400, content={"detail": f"파일 읽기 실패: {str(e)}"})

    df.columns = df.columns.str.strip()
    df = df.fillna("")

    # 필수 컬럼 확인
    for col in ["상품바코드", "발주번호", "물류센터"]:
        if col not in df.columns:
            return JSONResponse(status_code=400, content={"detail": f"'{col}' 열이 없습니다. PO 파일인지 확인하세요."})

    # ① 바코드 → 품목코드 변환
    df["상품바코드"] = df["상품바코드"].str.strip().str.replace(r"\.0$", "", regex=True)
    df["_품목코드"] = df["상품바코드"].map(lambda bc: barcode_to_code.get(bc, ""))

    # ② 납품부족사유 필터 — 제품 단종/제품 인상은 이카운트 제외
    reasons: dict = json.loads(shortage_reasons)
    excluded = {int(k) for k, v in reasons.items()
                if v.startswith("제품 단종") or v.startswith("제품 인상")}

    # 프론트에서 사유가 없더라도 마스터 자동 감지로 제외
    for i, row in df.iterrows():
        if i not in excluded:
            bc = str(row.get("상품바코드", "")).strip().replace(".0", "")
            prod_cd = barcode_to_code.get(bc, "")
            if bc in discontinued or (prod_cd and prod_cd in discontinued):
                excluded.add(i)
            elif bc in price_up or (prod_cd and prod_cd in price_up):
                excluded.add(i)

    excluded_cnt = len(excluded)
    df = df[~df.index.isin(excluded)].copy()

    # ③ 수량 컬럼 선택 (확정수량 우선, 없으면 발주수량)
    qty_col = "확정수량" if "확정수량" in df.columns else "발주수량"
    df[qty_col] = df[qty_col].str.strip().str.replace(",", "")

    # 유효 행만 (품목코드 있고, 수량 > 0)
    valid = df[
        (df["_품목코드"] != "") &
        (df[qty_col] != "") &
        (df[qty_col] != "0")
    ].copy()

    unmatched = int((df["_품목코드"] == "").sum())

    if valid.empty:
        return JSONResponse(status_code=400, content={
            "detail": f"전송할 유효한 데이터가 없습니다. (바코드 미매칭: {unmatched}건)"
        })

    # ③ 물류센터 ㄱ~ㅎ 순서 정렬 → 발주번호 순
    valid = valid.sort_values(["물류센터", "발주번호"]).reset_index(drop=True)

    # ④ 발주번호별 순번 할당 (같은 발주번호 = 같은 전표)
    doc_to_ser: dict = {}
    ser_counter = 1
    for doc_no in valid["발주번호"]:
        if doc_no not in doc_to_ser:
            doc_to_ser[doc_no] = str(ser_counter)
            ser_counter += 1

    # ⑤ BulkDatas 리스트 구성
    bulk_list = []
    for _, row in valid.iterrows():
        doc_no    = str(row["발주번호"]).strip()
        warehouse = str(row["물류센터"]).strip()
        qty_str   = str(row[qty_col]).replace(",", "").strip()
        supply_str = str(row.get("총발주 매입금", "")).replace(",", "").strip()
        vat_str    = str(row.get("부가세", "")).replace(",", "").strip()

        # 단가 = 공급가액 / 수량 (R = T / Q)
        try:
            price_val = round(float(supply_str) / float(qty_str)) if supply_str and qty_str and float(qty_str) != 0 else 0
        except:
            price_val = 0

        bulk_list.append({"BulkDatas": {
            "UPLOAD_SER_NO": doc_to_ser[doc_no],
            "IO_DATE":       today,
            "CUST":          "202308091",       # 거래처코드 고정
            "WH_CD":         "30",              # 출하창고 고정
            "EMP_CD":        staff_code,         # 담당자 코드
            "PROD_CD":       str(row["_품목코드"]).strip(),
            "PROD_DES":      "",   # 이카운트가 품목코드 기준으로 자동 입력
            "QTY":           qty_str,
            "PRICE":         str(price_val),
            "SUPPLY_AMT":    supply_str,
            "VAT_AMT":       vat_str,
            "REMARKS":       f"{warehouse} - {doc_no}",  # 예) 안산3 - 128117514
            "U_MEMO5":       f"{warehouse} - {doc_no}",  # 비고사항 = 문자형식5
        }})

    print(f"📦 전송 항목: {len(bulk_list)}개 | 미매칭: {unmatched}개 | 제외(단종/매입가인상): {excluded_cnt}개")

    # ⑥ 세션 발급 및 전송
    try:
        session_id, zone = await get_ecount_session()
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"이카운트 로그인 실패: {str(e)}"})

    sale_url = f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={session_id}"

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(sale_url, json={"SaleList": bulk_list})
        result = resp.json()

        # ⑦ 재고 조회 (전송 직후 같은 세션으로 조회)
        inv_map: dict = {}
        try:
            inv_url = (
                f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/"
                f"InventoryBalance/GetListInventoryBalanceStatusByLocation"
                f"?SESSION_ID={session_id}"
            )
            inv_resp = await client.post(inv_url, json={
                "BASE_DATE": today,
                "WH_CD":    "30",
                "PROD_CD":  "",
            })
            inv_data = inv_resp.json()
            print(f"📊 재고 조회: Status={inv_data.get('Status')}, "
                  f"TotalCnt={((inv_data.get('Data') or {}).get('TotalCnt', 0))}")
            for r in ((inv_data.get("Data") or {}).get("Result") or []):
                pc  = str(r.get("PROD_CD", "")).strip()
                try:
                    bq = float(str(r.get("BAL_QTY", "0") or "0"))
                except Exception:
                    bq = 0.0
                if pc:
                    inv_map[pc] = bq
        except Exception as e:
            print(f"⚠️ 재고 조회 실패: {e}")

    print(f"📨 이카운트 응답: {result}")

    status   = result.get("Status")
    data     = result.get("Data") or {}
    success  = data.get("SuccessCnt", 0)
    fail     = data.get("FailCnt", 0)
    slip_nos = data.get("SlipNos", [])
    errors   = []
    for rd in (data.get("ResultDetails") or []):
        if not rd.get("IsSuccess"):
            errors.append(rd.get("TotalError", ""))

    # ⑧ 항목별 재고 현황 구성
    items_result = []
    for item in bulk_list:
        bd       = item["BulkDatas"]
        prod_cd  = bd["PROD_CD"]
        bal_qty  = inv_map.get(prod_cd, None)
        low      = (bal_qty is not None) and (bal_qty <= 0)
        items_result.append({
            "upload_ser_no": bd["UPLOAD_SER_NO"],
            "remarks":       bd["U_MEMO5"],   # "물류센터 - 발주번호"
            "prod_cd":       prod_cd,
            "qty":           bd["QTY"],
            "bal_qty":       round(bal_qty) if bal_qty is not None else None,
            "low_stock":     low,
        })

    return JSONResponse(content={
        "status":       status,
        "total":        len(bulk_list),
        "success":      success,
        "fail":         fail,
        "slip_nos":     slip_nos,
        "errors":       errors,
        "unmatched":    unmatched,
        "excluded":     excluded_cnt,
        "items_result": items_result,
        "inv_checked":  len(inv_map) > 0,
    })


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
