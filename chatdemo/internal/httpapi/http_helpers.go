package httpapi

import (
	"encoding/json"
	"log"
	"net/http"

	"actor-chat-demo/internal/chat"
)

// فارسی: call فقط یک wrapper کوچک روی node.Call است.
// فارسی: اینجا همه requestهای HTTP به Registry actor فرستاده می‌شوند.
func (api *Handler) call(request any) (any, error) {
	return api.node.Call(api.registryPID, request)
}

// فارسی: writeCall الگوی تکراری Call + تبدیل جواب actor به HTTP response را جمع می‌کند.
func (api *Handler) writeCall(w http.ResponseWriter, request any) {
	result, err := api.call(request)
	writeActorResult(w, result, err)
}

// فارسی: decodeJSON body درخواست را به struct Go تبدیل می‌کند.
// فارسی: DisallowUnknownFields کمک می‌کند typo در JSON سریع مشخص شود.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, chat.Fail("invalid json: "+err.Error()))
		return false
	}

	return true
}

// فارسی: writeActorResult خروجی actor را به status مناسب HTTP تبدیل می‌کند.
// فارسی: خطای business با status 400 برمی‌گردد، ولی خطای runtime با 500.
func writeActorResult(w http.ResponseWriter, result any, err error) {
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, chat.Fail(err.Error()))
		return
	}

	if res, ok := result.(chat.Result); ok {
		if !res.OK {
			writeJSON(w, http.StatusBadRequest, res)
			return
		}

		writeJSON(w, http.StatusOK, res)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// فارسی: writeJSON تنها نقطه نوشتن response JSON است.
// فارسی: داشتن یک helper مرکزی باعث می‌شود format جواب‌ها یکدست بماند.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write json failed: %v", err)
	}
}
