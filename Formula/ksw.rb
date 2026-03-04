class Ksw < Formula
  desc "AI-powered interactive Kubernetes context switcher"
  homepage "https://github.com/YonierGomez/ksw"
  url "https://github.com/YonierGomez/ksw/archive/refs/tags/v1.5.0.tar.gz"
  sha256 "bf61ae0f62d93775bd51b987790df0581cce0d894d1728b70b83a7bbe9f05631"
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
