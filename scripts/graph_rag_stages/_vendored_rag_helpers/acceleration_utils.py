"""
Hardware acceleration utilities for the pipeline (vendored copy).

Provides Apple-Silicon optimisation when available, with CPU fallback.
No external dependency on the legacy RAG_stages tree.
"""
import os
import platform
import multiprocessing as mp
from typing import Optional, Callable, Any, List
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import asyncio

log = logging.getLogger(__name__)


class HardwareAccelerator:
    """Detects and manages hardware acceleration capabilities."""

    def __init__(self):
        self.is_apple_silicon = self._detect_apple_silicon()
        self.cpu_count = mp.cpu_count()
        self.optimal_workers = self._calculate_optimal_workers()

        # Clamp thread-hungry BLAS libs on Mac to avoid oversubscription
        if self.is_apple_silicon:
            os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
            os.environ.setdefault("MKL_NUM_THREADS", "1")
            os.environ.setdefault("OMP_NUM_THREADS", "1")

        log.info(
            "Hardware: %s, logical cores: %d, optimal workers: %d",
            "Apple Silicon" if self.is_apple_silicon else platform.processor(),
            self.cpu_count,
            self.optimal_workers,
        )

    # ── private helpers ──────────────────────────────────────────────

    def _detect_apple_silicon(self) -> bool:
        if platform.system() != "Darwin":
            return False
        try:
            import subprocess

            out = subprocess.run(
                ["sysctl", "-n", "hw.optional.arm64"],
                capture_output=True,
                text=True,
                check=False,
            )
            return out.stdout.strip() == "1"
        except Exception:
            return False

    def _calculate_optimal_workers(self) -> int:
        if self.is_apple_silicon:
            # leave some efficiency-cores free
            return max(1, int(self.cpu_count * 0.75))
        # generic CPU – leave one core for the OS
        return max(1, self.cpu_count - 1)

    # ── public factory helpers ───────────────────────────────────────

    def get_process_pool(self, max_workers: Optional[int] = None) -> ProcessPoolExecutor:
        """Process-pool tuned for CPU-bound tasks (spawn ctx for macOS)."""
        workers = max_workers or self.optimal_workers
        return ProcessPoolExecutor(
            max_workers=workers, mp_context=mp.get_context("spawn")
        )

    def get_thread_pool(self, max_workers: Optional[int] = None) -> ThreadPoolExecutor:
        """Thread-pool tuned for I/O-bound tasks."""
        workers = max_workers or min(32, self.cpu_count * 4)
        return ThreadPoolExecutor(max_workers=workers)


# single global instance – import-side-effect is fine
hardware = HardwareAccelerator()


# ────────────────────────────────────────────────────────────────────
# convenience async wrappers
# ────────────────────────────────────────────────────────────────────
async def run_cpu_bound_concurrent(
    func: Callable[[Any], Any],
    items: List[Any],
    *,
    max_workers: Optional[int] = None,
    desc: str = "Processing",
) -> List[Any]:
    """Run *func* over *items* concurrently in a process pool."""
    loop = asyncio.get_event_loop()
    with hardware.get_process_pool(max_workers) as pool:
        futs = [loop.run_in_executor(pool, func, it) for it in items]
        from tqdm.asyncio import tqdm_asyncio

        return await tqdm_asyncio.gather(*futs, desc=desc)


async def run_io_bound_concurrent(
    func: Callable[[Any], Any],
    items: List[Any],
    *,
    max_workers: Optional[int] = None,
    desc: str = "Processing",
) -> List[Any]:
    """Run *func* over *items* concurrently in a thread pool."""
    loop = asyncio.get_event_loop()
    with hardware.get_thread_pool(max_workers) as pool:
        futs = [loop.run_in_executor(pool, func, it) for it in items]
        from tqdm.asyncio import tqdm_asyncio

        return await tqdm_asyncio.gather(*futs, desc=desc) 