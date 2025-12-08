
from __future__ import annotations

import asyncio
import json
import logging
import traceback
from contextlib import asynccontextmanager
from typing import Iterable, Optional, Sequence, Union

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

FACEBOOK_API = "https://graph.facebook.com/v23.0"

# ---------------------------------------------------------------------------
# Core utils
# ---------------------------------------------------------------------------


@asynccontextmanager
async def get_client(timeout: float = 60.0):
    """Yield a shared httpx.AsyncClient with sane defaults."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        yield client


def _build_url(page_id: str, edge: str) -> str:
    return f"{FACEBOOK_API}/{page_id}/{edge}"


fb_retry = retry(
    stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True
)

# ---------------------------------------------------------------------------
# 1) TEXT
# ---------------------------------------------------------------------------


@fb_retry
async def post_text_to_facebook_page(page_id: str, access_token: str, message: str) -> dict:
    """Publish plain‑text status."""
    async with get_client() as client:
        r = await client.post(
            _build_url(page_id, "feed"),
            params={"message": message, "access_token": access_token},
        )
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# 2) SINGLE PHOTO (by remote URL)
# ---------------------------------------------------------------------------


@fb_retry
async def post_photo_to_facebook_page(
    page_id: str, access_token: str, image_url: str, caption: str = ""
) -> dict:
    """Publish a single photo **without** downloading — just pass the remote URL."""
    async with get_client() as client:
        r = await client.post(
            _build_url(page_id, "photos"),
            data={"url": image_url, "caption": caption, "access_token": access_token},
        )
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# 3) MULTI‑PHOTO ALBUM/POST
# ---------------------------------------------------------------------------


async def post_multiple_photos_to_facebook_page(
    page_id: str,
    access_token: str,
    image_urls: Sequence[str],
    caption: str = "",
) -> dict:
    """Create a single feed post that contains *multiple* photos via remote URLs.

    Steps:
    1. Upload each image with `published=false` to retrieve its `id`.
    2. Create a `/feed` post with `attached_media[]` referencing those IDs.
    """

    if not image_urls:
        raise ValueError("List ảnh trống 😭")

    async with get_client() as client:
        # 1️⃣ Upload images in parallel
        async def _upload(url: str) -> Optional[str]:
            try:
                resp = await client.post(
                    _build_url(page_id, "photos"),
                    data={
                        "access_token": access_token,
                        "published": "false",
                        "url": url,
                    },
                )
                resp.raise_for_status()
                return resp.json().get("id")
            except Exception as exc:  # pragma: no cover
                logger.warning("Skip image %s: %s", url, exc)
                return None

        media_ids = [mid for mid in await asyncio.gather(*map(_upload, image_urls)) if mid]
        if not media_ids:
            raise RuntimeError("Không upload được ảnh nào 🤯")

        # 2️⃣ Create the multi‑photo post
        attach = {f"attached_media[{i}]": json.dumps({"media_fbid": mid}) for i, mid in enumerate(media_ids)}
        feed_resp = await client.post(
            _build_url(page_id, "feed"),
            data={"message": caption, "access_token": access_token, **attach},
        )
        feed_resp.raise_for_status()
        return {"post": feed_resp.json(), "media_fbids": media_ids}


# ---------------------------------------------------------------------------
# 4) VIDEO (direct URL → no download)
# ---------------------------------------------------------------------------


@fb_retry
async def post_video_to_facebook_page(
    page_id: str,
    access_token: str,
    video_url: Union[str, Sequence[str]],
    description: Optional[str] = None,
) -> dict:
    """Upload *and* publish a video in one shot using the `file_url` shortcut.

    Facebook will fetch the binary itself. Big time saver vs. streaming bytes.
    If `file_url` flow fails (older apps, some private URLs), we gracefully
    fall back to the three‑phase *resumable* upload.
    """

    vid_src = video_url[0] if isinstance(video_url, (list, tuple)) else video_url

    # --- Fast path: file_url -------------------------------------------------
    async with get_client(timeout=300) as client:
        data = {"access_token": access_token, "file_url": vid_src}
        if description:
            data["description"] = description
        resp = await client.post(_build_url(page_id, "videos"), data=data)
        if resp.status_code == 200:
            return resp.json()

        logger.warning("file_url upload failed (%s). Fallback to resumable.", resp.text)

        # --- Fallback: resumable chunked upload -----------------------------
        # Step 1️⃣: start session
        # First we need the file size ‑ ask Facebook to probe the URL for us.
        start_payload = {
            "access_token": access_token,
            "upload_phase": "start",
            "file_url": vid_src,
        }
        start = await client.post(_build_url(page_id, "videos"), data=start_payload)
        start.raise_for_status()
        sess = start.json()
        session_id = sess.get("upload_session_id")
        video_id = sess.get("video_id")
        start_offset = int(sess.get("start_offset", 0))
        end_offset = int(sess.get("end_offset", 0))

        if not session_id:
            raise RuntimeError(f"Unexpected start response: {sess}")

        # Step 2️⃣: because we supplied `file_url`, Facebook will pull the media
        # itself — we just need to tell it we’re done. If offsets are both 0 the
        # server already finished fetching; otherwise we still need to call
        # `transfer` with empty body to kick fetch‑by‑URL.
        if start_offset != end_offset:
            transfer_payload = {
                "access_token": access_token,
                "upload_phase": "transfer",
                "upload_session_id": session_id,
                "start_offset": start_offset,
                "video_file_chunk": "",  # empty => server grabs file_url
            }
            transfer = await client.post(_build_url(page_id, "videos"), data=transfer_payload)
            transfer.raise_for_status()

        # Step 3️⃣: finish & publish
        finish_payload = {
            "access_token": access_token,
            "upload_phase": "finish",
            "upload_session_id": session_id,
            "video_state": "PUBLISHED",
        }
        if description:
            finish_payload["description"] = description
        finish = await client.post(_build_url(page_id, "videos"), data=finish_payload)
        finish.raise_for_status()
        return {"video_id": video_id, "publish": finish.json()}


# ---------------------------------------------------------------------------
# 5) FACEBOOK REEL (keep old flow ‑ still needs upload_url)
# ---------------------------------------------------------------------------


async def post_reel_to_facebook_page(
    page_id: str,
    access_token: str,
    video_url: str,
    description: str = "",
    cover_url: str | None = None,
) -> dict:
    """Publish a Reel via the dedicated `/video_reels` edge."""

    async with get_client() as client:
        # 1️⃣ start
        init = await client.post(
            f"{FACEBOOK_API}/{page_id}/video_reels",
            json={"upload_phase": "start", "access_token": access_token},
        )
        init.raise_for_status()
        meta = init.json()
        upload_url: str = meta["upload_url"]
        video_id: str = meta["video_id"]

    # 2️⃣ Let Facebook fetch the file directly — send URL in header.
    async with get_client(timeout=300) as client:
        upload = await client.post(
            upload_url,
            headers={"Authorization": f"OAuth {access_token}", "file_url": video_url},
        )
        upload.raise_for_status()

    # 3️⃣ finish
    finish_payload = {
        "upload_phase": "finish",
        "access_token": access_token,
        "video_id": video_id,
        "video_state": "PUBLISHED",
    }
    if description:
        finish_payload["description"] = description
    if cover_url:
        finish_payload["thumb"] = cover_url

    async with get_client() as client:
        fin = await client.post(f"{FACEBOOK_API}/{page_id}/video_reels", json=finish_payload)
        fin.raise_for_status()
        return {"video_id": video_id, "publish": fin.json()}


# ---------------------------------------------------------------------------
# 6) POLL VIDEO STATUS
# ---------------------------------------------------------------------------


async def poll_facebook_video_status(
    video_id: str,
    access_token: str,
    interval: float = 2.0,
    attempts: int = 2,
) -> dict:
    """
    Gọi API status của video 1–2 lần. Raise nếu gặp lỗi.
    """
    status_url = f"{FACEBOOK_API}/{video_id}?fields=status&access_token={access_token}"
    last_status = None

    async with get_client() as client:
        for attempt in range(attempts):
            try:
                resp = await client.get(status_url)
                resp.raise_for_status()
                status_data = resp.json()
                last_status = status_data
                state = status_data.get("status", {}).get("video_status")

                logger.debug(f"[poll {attempt+1}/{attempts}] video_status = {state}")

                if state in {"published", "ready"}:
                    return {"final": True, "status": status_data}
                elif state == "error":
                    raise RuntimeError(
                        f"Video lỗi (status=error): {json.dumps(status_data, ensure_ascii=False)}"
                    )

            except Exception as e:
                logger.warning(f"[poll error] attempt {attempt+1}: {e}")

            await asyncio.sleep(interval)

    raise RuntimeError(
        f"Không xác định được trạng thái video sau {attempts} lần. Last known: {json.dumps(last_status or {}, ensure_ascii=False)}"
    )

__all__ = [
    "post_text_to_facebook_page",
    "post_photo_to_facebook_page",
    "post_multiple_photos_to_facebook_page",
    "post_video_to_facebook_page",
    "post_reel_to_facebook_page",
    "poll_facebook_video_status",
]
