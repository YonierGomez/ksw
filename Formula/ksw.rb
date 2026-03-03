class Ksw < Formula
  desc "AI-powered interactive Kubernetes context switcher"
  homepage "https://github.com/YonierGomez/ksw"
  url "https://github.com/YonierGomez/ksw/archive/refs/tags/v1.4.1.tar.gz"
  sha256 "40ecfc8b60a357d6b965ed268fe500a22c86e3e5560900fafd0cded515d5c991"
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
