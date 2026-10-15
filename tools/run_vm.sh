#!/usr/bin/env bash
# ARM64 "Raspberry Pi-class" VM: 4 cores, 2 GB RAM, Cortex-A72 (same CPU as a Pi 4)
cd ~/maya
exec qemu-system-aarch64 -M virt -cpu cortex-a72 -smp 4 -m 2048 \
  -bios /usr/share/edk2/aarch64/QEMU_EFI.fd \
  -drive if=virtio,file=device.qcow2,format=qcow2,cache=none \
  -netdev user,id=net0 \
  -device virtio-net-pci,netdev=net0 \
  -monitor unix:/tmp/qemu-mon,server,nowait \
  -nographic
