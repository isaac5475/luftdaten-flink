# utils/progress_bar.sh
progress_bar() {
    local total="$1"
    local width=40
    local start_ts
    start_ts=$(date +%s)

    while true; do
        local now elapsed pct filled empty
        now=$(date +%s)
        elapsed=$((now - start_ts))
        [ "$elapsed" -ge "$total" ] && elapsed="$total"

        pct=$(( elapsed * 100 / total ))
        filled=$(( elapsed * width / total ))
        empty=$(( width - filled ))

        printf "\r["
        printf "%0.s#" $(seq 1 "$filled") 2>/dev/null
        printf "%0.s." $(seq 1 "$empty") 2>/dev/null
        printf "] %3d%% (%ds / %ds)" "$pct" "$elapsed" "$total"

        [ "$elapsed" -ge "$total" ] && break
        sleep 1
    done
    echo ""
}
