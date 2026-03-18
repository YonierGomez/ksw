class Ksw < Formula
  desc "AI-powered interactive Kubernetes context switcher"
  homepage "https://github.com/YonierGomez/ksw"
  url "https://github.com/YonierGomez/ksw/archive/refs/tags/v1.6.0.tar.gz"
  sha256 "4ee7563b27b481d2bac09503ab9d5e34d21465fe811ce169f39b878b3d02c894"
  license "MIT"

  depends_on "go" => :build
  depends_on "kubernetes-cli"

  def install
    system "go", "build", "-ldflags", "-s -w", "-o", bin/"ksw", "."
  end

  test do
    assert_match "ksw v#{version}", shell_output("#{bin}/ksw -v")
  end
end
