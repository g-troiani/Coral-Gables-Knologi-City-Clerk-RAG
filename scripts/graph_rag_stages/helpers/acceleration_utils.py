"""
Hardware-aware executors shared by concurrent helper stages.
(Revendored copy – original RAG_stages module stays frozen.)
"""
from __future__ import annotations

import asyncio, logging, multiprocessing as mp, os, platform
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, List, Optional

log = logging.getLogger(__name__)


class HardwareAccelerator:
    def __init__(self) -> None:
        self.is_macos_arm = self._detect_apple_silicon()
        self.cores = mp.cpu_count()
        self.opt_workers = self._optimal_workers()

        if self.is_macos_arm:  # clamp BLAS threads for M-series
            os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
            os.environ.setdefault("MKL_NUM_THREADS", "1")
            os.environ.setdefault("OMP_NUM_THREADS", "1")

        log.info(
            "Hardware: %s — cores=%d, workers=%d",
            "Apple-Silicon" if self.is_macos_arm else platform.processor(),
            self.cores,
            self.opt_workers,
        )

    # ── detection helpers ────────────────────────────────────────
    @staticmethod
    def _detect_apple_silicon() -> bool:
        if platform.system() != "Darwin":
            return False
        try:
            import subprocess

            return (
                subprocess.run(
                    ["sysctl", "-n", "hw.optional.arm64"],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                == "1"
            )
        except Exception:
            return False

    def _optimal_workers(self) -> int:
        if self.is_macos_arm:
            return max(1, int(self.cores * 0.75))
        return max(1, self.cores - 1)

    # ── pool factories ───────────────────────────────────────────
    def get_process_pool(
        self, max_workers: int | None = None
    ) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(
            max_workers=max_workers or self.opt_workers,
            mp_context=mp.get_context("spawn"),
        )

    def get_thread_pool(
        self, max_workers: int | None = None
    ) -> ThreadPoolExecutor:
        return ThreadPoolExecutor(max_workers=max_workers or min(32, self.cores * 4))


# singleton
hardware = HardwareAccelerator()

# ── async helpers for convenience ────────────────────────────────
from tqdm.asyncio import tqdm_asyncio


async def run_cpu_bound_concurrent(
    func: Callable[[Any], Any],
    items: List[Any],
    *,
    max_workers: Optional[int] = None,
    desc: str = "Processing",
) -> List[Any]:
    loop = asyncio.get_event_loop()
    with hardware.get_process_pool(max_workers) as pool:
        futs = [loop.run_in_executor(pool, partial(func, it)) for it in items]
        return await tqdm_asyncio.gather(*futs, desc=desc)


async def run_io_bound_concurrent(
    func: Callable[[Any], Any],
    items: List[Any],
    *,
    max_workers: Optional[int] = None,
    desc: str = "Processing",
) -> List[Any]:
    loop = asyncio.get_event_loop()
    with hardware.get_thread_pool(max_workers) as pool:
        futs = [loop.run_in_executor(pool, partial(func, it)) for it in items]
        return await tqdm_asyncio.gather(*futs, desc=desc) 